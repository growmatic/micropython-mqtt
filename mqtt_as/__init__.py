# mqtt_as.py Asynchronous version of umqtt.robust
# (C) Copyright Peter Hinch 2017-2025.
# Released under the MIT licence.
#
# PATCH local (device light, 2026-08-25): este device usa network.ESP_HOSTED()
# (WiFi via co-processador C3/SPI-FD), NAO network.WLAN(STA_IF) nativo -- e a
# conexao/reconexao ja e gerenciada por light_mesh.py, assincrona, com sua
# propria logica de retry. Empiricamente confirmado que WLAN(STA_IF) neste
# firmware e uma interface totalmente separada e inativa, sem relacao com o
# ESP_HOSTED() conectado -- entao o gerenciamento de WiFi nativo desta lib
# (hardcoded em network.WLAN(STA_IF)) nao serve aqui.
# Mudancas: (1) config["wifi_if"] permite injetar uma interface ja conectada
# (nosso ESP_HOSTED()) no lugar do WLAN nativo; (2) quando injetada, esta lib
# PARA de tentar conectar/desconectar WiFi por conta propria (isso e trabalho
# do light_mesh.py) -- so espera/observa o estado, e cuida apenas da
# reconexao em nivel de MQTT (que e o que realmente queremos dela).

# Pyboard D support added also RP2/default
# Various improvements contributed by Kevin Köck
# V5 support added by Bob Veringa.
# Also other contributors.

import gc
import socket
import struct
import time

gc.collect()
from binascii import hexlify
import asyncio

gc.collect()
from time import ticks_ms, ticks_diff
from errno import EINPROGRESS, ETIMEDOUT, EAGAIN

gc.collect()
from micropython import const
from machine import unique_id
import network

gc.collect()
from sys import platform, implementation

VERSION = (0, 8, 5)
# Default initial size for input messge buffer. Increase this if large messages
# are expected, but rarely, to avoid big runtime allocations
IBUFSIZE = 50
# By default the callback interface returns and incoming message as bytes.
# For performance reasons with large messages it may return a memoryview.
MSG_BYTES = True

# Legitimate errors while waiting on a socket. See uasyncio __init__.py open_connection().
ESP32 = platform == "esp32"
RP2 = platform == "rp2"
NINA = RP2 and implementation._machine.startswith("Arduino")  # ublox Nina radio
if ESP32:
    # https://forum.micropython.org/viewtopic.php?f=16&t=3608&p=20942#p20942
    # PATCH local (2026-08-26): EAGAIN(11) adicionado -- e o que o socket TLS
    # (extmod/modtls_mbedtls.c, socket_read/socket_write) levanta quando o
    # handshake ainda nao terminou (MBEDTLS_ERR_SSL_WANT_READ/WRITE vira
    # MP_EWOULDBLOCK == MP_EAGAIN == 11 nesse build). Sem isso aqui, _as_write/
    # _as_read tratam esse retorno como erro fatal em vez de tentar de novo --
    # ver do_handshake_on_connect=False mais abaixo e memoria
    # light_mqtt_freeze_unreachable_host.md.
    BUSY_ERRORS = [EINPROGRESS, ETIMEDOUT, EAGAIN, 118, 119]  # Add in weird ESP32 errors
elif RP2 and not NINA:
    BUSY_ERRORS = [EINPROGRESS, ETIMEDOUT, -110]
else:
    BUSY_ERRORS = [EINPROGRESS, ETIMEDOUT]

ESP8266 = platform == "esp8266"
PYBOARD = platform == "pyboard"


# Default "do little" coro for optional user replacement
async def eliza(*_):  # e.g. via set_wifi_handler(coro): see test program
    await asyncio.sleep_ms(0)


class MsgQueue:
    def __init__(self, size):
        self._q = [0 for _ in range(max(size, 4))]
        self._size = size
        self._wi = 0
        self._ri = 0
        self._evt = asyncio.Event()
        self.discards = 0

    def put(self, *v):
        self._q[self._wi] = v
        self._evt.set()
        self._wi = (self._wi + 1) % self._size
        if self._wi == self._ri:  # Would indicate empty
            self._ri = (self._ri + 1) % self._size  # Discard a message
            self.discards += 1

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._ri == self._wi:  # Empty
            self._evt.clear()
            await self._evt.wait()
        r = self._q[self._ri]
        self._ri = (self._ri + 1) % self._size
        return r


config = {
    "client_id": hexlify(unique_id()),
    "server": None,
    "port": 0,
    "user": "",
    "password": "",
    "keepalive": 60,
    "ping_interval": 0,
    "ssl": False,
    "ssl_params": {},
    "response_time": 10,
    "clean_init": True,
    "clean": True,
    "max_repubs": 4,
    "will": None,
    "subs_cb": lambda *_: None,
    "wifi_coro": eliza,
    "connect_coro": eliza,
    "ssid": None,
    "wifi_pw": None,
    "queue_len": 0,
    "gateway": False,
    "mqttv5": False,
    "mqttv5_con_props": None,
    # PATCH growmatic: properties MQTTv5 mescladas em TODO PUBLISH (ex.: user
    # properties de identidade do device, auditadas pelo servidor em cada
    # mensagem). User properties (0x26) do publish() somam com estas.
    "pub_props": None,
    "wifi_if": None,  # PATCH local: interface ja conectada (ex. ESP_HOSTED()),
                       # no lugar do WLAN(STA_IF) nativo. Ver nota no topo do arquivo.
    "nak_cb": None,  # PATCH local: nak_cb(kind, topic, reason_code) quando o
                     # broker NEGA um SUBSCRIBE/UNSUBSCRIBE/PUBLISH (>=0x80).
}


class MQTTException(Exception):
    pass


def pid_gen():
    pid = 0
    while True:
        pid = pid + 1 if pid < 65535 else 1
        yield pid


def qos_check(qos):
    if not (qos == 0 or qos == 1):
        raise ValueError("Only qos 0 and 1 are supported.")


# Populate a byte array with a variable byte integer. Args: buf the bytearray,
# offs: start offset. x the value. Returns the end offset.
# 1-4 bytes allowed, encoding up to 268,435,455 (V3.1.1 table 2.4). No point trapping this.
def vbi(buf: bytearray, offs: int, x: int):
    buf[offs] = x & 0x7F
    if x := x >> 7:
        buf[offs] |= 0x80
    return vbi(buf, offs + 1, x) if x else (offs + 1)


encode_properties = None
decode_properties = None


class MQTT_base:
    REPUB_COUNT = 0  # TEST
    DEBUG = False

    def __init__(self, config):
        self._events = config["queue_len"] > 0
        # MQTT config
        self._client_id = config["client_id"]
        self._user = config["user"]
        self._pswd = config["password"]
        self._keepalive = config["keepalive"]
        if self._keepalive >= 65536:
            raise ValueError("invalid keepalive time")
        self._response_time = config["response_time"] * 1000  # Repub if no PUBACK received (ms).
        self._max_repubs = config["max_repubs"]
        self._clean_init = config["clean_init"]  # clean_session state on first connection
        self._clean = config["clean"]  # clean_session state on reconnect
        will = config["will"]
        if will is None:
            self._lw_topic = False
        else:
            self._set_last_will(*will)
        # WiFi config
        self._ssid = config["ssid"]  # Required for ESP32 / Pyboard D. Optional ESP8266
        self._wifi_pw = config["wifi_pw"]
        self._ssl = config["ssl"]
        self._ssl_params = config["ssl_params"]
        # Callbacks and coros
        if self._events:
            self.up = asyncio.Event()
            self.down = asyncio.Event()
            self.queue = MsgQueue(config["queue_len"])
            self._cb = self.queue.put
        else:  # Callbacks
            self._cb = config["subs_cb"]
            self._wifi_handler = config["wifi_coro"]
            self._connect_handler = config["connect_coro"]
        # Network
        self.port = config["port"]
        if self.port == 0:
            self.port = 8883 if self._ssl else 1883
        self.server = config["server"]
        if self.server is None:
            raise ValueError("no server specified.")
        self._sock = None
        # PATCH local: WiFi externo (ESP_HOSTED) ja ativo/gerenciado pelo
        # light_mesh.py -- nao mexe em .active()/.connect()/.disconnect() dele.
        self._external_wifi = config["wifi_if"] is not None
        if self._external_wifi:
            self._sta_if = config["wifi_if"]
        else:
            self._sta_if = network.WLAN(network.STA_IF)
            self._sta_if.active(True)
        if config["gateway"]:  # Called from gateway (hence ESP32).
            import aioespnow  # Set up ESPNOW

            while not (sta := self._sta_if).active():
                time.sleep(0.1)
            sta.config(pm=sta.PM_NONE)  # No power management
            sta.active(True)
            self._espnow = aioespnow.AIOESPNow()  # Returns AIOESPNow enhanced with async support
            self._espnow.active(True)

        self.newpid = pid_gen()
        self.rcv_pids = set()  # PUBACK and SUBACK pids awaiting ACK response
        # PATCH local (2026-09-24): pids que vieram NEGADOS (reason code
        # >=0x80), nao so "sem resposta ainda" -- rcv_pids sozinho nao
        # diferencia "ack de sucesso" de "nak", entao _await_pid() precisa
        # consultar isto pra nao devolver True (sucesso) pra um pid que na
        # verdade foi rejeitado. Ver _await_pid() e o handling de PUBACK/
        # [UN]SUBACK em wait_msg() mais abaixo.
        self._nak_pids = {}
        self._nak_cb = config.get("nak_cb")
        self.last_rx = ticks_ms()  # Time of last communication from broker
        self.lock = asyncio.Lock()
        self._ibuf = bytearray(IBUFSIZE)
        self._mvbuf = memoryview(self._ibuf)

        self.mqttv5 = config.get("mqttv5")
        self.mqttv5_con_props = config.get("mqttv5_con_props")
        self._pub_props = config.get("pub_props")
        self.topic_alias_maximum = 0

        if self.mqttv5:
            global encode_properties, decode_properties
            from .mqtt_v5_properties import encode_properties, decode_properties  # noqa

    def _set_last_will(self, topic, msg, retain=False, qos=0):
        qos_check(qos)
        if not topic:
            raise ValueError("Empty topic.")
        self._lw_topic = topic
        self._lw_msg = msg
        self._lw_qos = qos
        self._lw_retain = retain

    def dprint(self, msg, *args):
        if self.DEBUG:
            print(msg % args)

    def _timeout(self, t):
        return ticks_diff(ticks_ms(), t) > self._response_time

    async def _as_read(self, n, sock=None):  # OSError caught by superclass
        if sock is None:
            sock = self._sock
        # Ensure input buffer is big enough to hold data. It keeps the new size
        oflow = n - len(self._ibuf)
        if oflow > 0:  # Grow the buffer and re-create the memoryview
            # Avoid too frequent small allocations by adding some extra bytes
            self._ibuf.extend(bytearray(oflow + 50))
            self._mvbuf = memoryview(self._ibuf)
        buffer = self._mvbuf
        size = 0
        t = ticks_ms()
        while size < n:
            if self._timeout(t) or not self.isconnected():
                raise OSError(-1, "Timeout on socket read")
            try:
                msg_size = sock.readinto(buffer[size:], n - size)
            except OSError as e:  # ESP32 issues weird 119 errors here
                msg_size = None
                if e.args[0] not in BUSY_ERRORS:
                    raise
            if msg_size == 0:  # Connection closed by host
                raise OSError(-1, "Connection closed by host")
            if msg_size is not None:  # data received
                size += msg_size
                t = ticks_ms()
                self.last_rx = ticks_ms()
            await asyncio.sleep_ms(0)
        return buffer[:n]

    async def _as_write(self, bytes_wr, length=0, sock=None):
        if sock is None:
            sock = self._sock

        # Wrap bytes in memoryview to avoid copying during slicing
        bytes_wr = memoryview(bytes_wr)
        if length:
            bytes_wr = bytes_wr[:length]
        t = ticks_ms()
        while bytes_wr:
            if self._timeout(t) or not self.isconnected():
                raise OSError(-1, "Timeout on socket write")
            try:
                n = sock.write(bytes_wr)
            except OSError as e:  # ESP32 issues weird 119 errors here
                n = 0
                if e.args[0] not in BUSY_ERRORS:
                    raise
            if n:
                t = ticks_ms()
                bytes_wr = bytes_wr[n:]
            await asyncio.sleep_ms(0)

    async def _send_str(self, s):
        await self._as_write(struct.pack("!H", len(s)))
        await self._as_write(s)

    # Receive a Variable Byte Integer and decode.
    async def _recv_len(self, d=0, i=0):
        s = (await self._as_read(1))[0]
        d |= (s & 0x7F) << (i * 7)
        return await self._recv_len(d, i + 1) if (s & 0x80) else (d, i + 1)

    async def _connect(self, clean):
        mqttv5 = self.mqttv5  # Cache local
        self._sock = socket.socket()
        self._sock.setblocking(False)
        try:
            self._sock.connect(self._addr)
        except OSError as e:
            if e.args[0] not in BUSY_ERRORS:
                raise
        await asyncio.sleep_ms(0)
        self.dprint("Connecting to broker.")
        if self._ssl:
            # PATCH local: usa SSLContext + load_cert_chain/load_verify_locations
            # (padrao ja validado neste firmware para mTLS real, ver
            # socket_tests/util.py) em vez do ssl.wrap_socket(**kwargs) original
            # -- os nomes de kwargs aceitos por wrap_socket() variam entre
            # builds de MicroPython e nao foram validados aqui.
            # ssl_params esperado: {"cadata":..., "client_cert":path,
            # "client_key":path, "server_hostname":...}
            #
            # PATCH local (2026-08-26): wrap_socket() com handshake completo
            # (do_handshake_on_connect=True, o default) roda numa THREAD
            # separada (_thread, outro nucleo do ESP32) em vez de direto aqui.
            # Motivo: mesmo tratando WANT_READ/WANT_WRITE como "tenta de novo"
            # (do_handshake_on_connect=False + EAGAIN em BUSY_ERRORS, ver
            # BUSY_ERRORS acima), a ETAPA FINAL do handshake -- verificacao de
            # certificado + troca de chave, crypto de verdade -- roda inteira
            # numa unica chamada sincrona do mbedTLS (~3s medido neste
            # hardware) que nao tem como ser interrompida/fatiada em Python.
            # Isso travava o loop de asyncio (LVGL, encoder, clock_task) por
            # esse tanto TODA VEZ que uma conexao/reconexao tinha sucesso --
            # pouca coisa numa conexao isolada, mas se cair em retry
            # frequente (rede instavel, broker caindo e voltando) o
            # travamento de ~3s se repetiria a cada tentativa bem-sucedida.
            # Rodar numa thread separada resolve de vez, sem depender de
            # nenhum comportamento fino do mbedTLS: o handshake pode demorar
            # o quanto precisar (rede lenta, host inalcancavel, crypto lenta)
            # que o loop principal nunca sente. Ver memoria
            # light_mqtt_freeze_unreachable_host.md.
            import ssl
            import _thread

            p = self._ssl_params
            raw_sock = self._sock
            handshake = {}
            flag = asyncio.ThreadSafeFlag()

            def _handshake_worker():
                try:
                    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ctx.verify_mode = ssl.CERT_REQUIRED
                    if p.get("cadata") is not None:
                        ctx.load_verify_locations(cadata=p["cadata"])
                    if p.get("client_cert") and p.get("client_key"):
                        ctx.load_cert_chain(p["client_cert"], p["client_key"])
                    # do_handshake_on_connect=True (default) -- bloqueante,
                    # mas isso e o esperado/ok aqui: estamos numa thread so
                    # pra isso, o loop principal continua livre.
                    handshake["sock"] = ctx.wrap_socket(
                        raw_sock, server_hostname=p.get("server_hostname"))
                except Exception as e:
                    handshake["error"] = e
                flag.set()

            # PATCH local (2026-08-26): stack maior pra thread do handshake.
            # Crash reproduzido (Guru Meditation StoreProhibited) rodando a
            # stack completa com o stack DEFAULT do _thread -- RSA-4096
            # (usado pelos certs deste projeto) usa bastante stack dentro do
            # mbedTLS, e o default do MicroPython/ESP32 e pequeno demais.
            # 32KB e generoso (sobra PSRAM/heap nesta placa) e a thread
            # termina logo depois do handshake, nao fica ocupando memoria.
            _thread.stack_size(16 * 1024)
            _thread.start_new_thread(_handshake_worker, ())
            await flag.wait()
            if "error" in handshake:
                raise handshake["error"]
            self._sock = handshake["sock"]
        premsg = bytearray(b"\x10\0\0\0\0\0")
        msg = bytearray(b"\x04MQTT\x00\0\0\0")
        msg[5] = 0x05 if mqttv5 else 0x04

        sz = 10 + 2 + len(self._client_id)
        msg[6] = clean << 1
        if self._user:
            sz += 2 + len(self._user) + 2 + len(self._pswd)
            msg[6] |= 0xC0
        if self._keepalive:
            msg[7] |= self._keepalive >> 8
            msg[8] |= self._keepalive & 0x00FF
        if self._lw_topic:
            sz += 2 + len(self._lw_topic) + 2 + len(self._lw_msg)
            if mqttv5:
                # Extra for the will properties
                sz += 1
            msg[6] |= 0x4 | (self._lw_qos & 0x1) << 3 | (self._lw_qos & 0x2) << 3
            msg[6] |= self._lw_retain << 5

        if mqttv5:
            properties = encode_properties(self.mqttv5_con_props)
            sz += len(properties)

        i = vbi(premsg, 1, sz)  # sz -> Variable Byte Integer
        await self._as_write(premsg, i + 1)
        await self._as_write(msg)
        if mqttv5:
            await self._as_write(properties)

        await self._send_str(self._client_id)
        if self._lw_topic:
            if mqttv5:
                # We don't support will properties, so we send 0x00 for properties length
                await self._as_write(b"\x00")
            await self._send_str(self._lw_topic)
            await self._send_str(self._lw_msg)
        if self._user:
            await self._send_str(self._user)
            await self._send_str(self._pswd)
        # Await CONNACK
        # read causes ECONNABORTED if broker is out; triggers a reconnect.
        del premsg, msg
        packet_type = await self._as_read(1)
        if packet_type[0] != 0x20:
            raise OSError(-1, "CONNACK not received")
        # The connect packet has changed, so size might be different now. But
        # we can still handle it the same for 3.1.1 and v5
        sz, _ = await self._recv_len()
        if not mqttv5 and sz != 2:
            raise OSError(-1, "Invalid CONNACK packet")

        # Only read the first 2 bytes, as properties have their own length
        connack_resp = await self._as_read(2)

        # Connect ack flags
        if connack_resp[0] != 0:
            raise OSError(-1, "CONNACK flags not 0")
        # Reason code
        if connack_resp[1] != 0:
            # On MQTTv5 Reason codes below 128 may need to be handled
            # differently. For now, we just raise an error. Spec is a bit weird
            # on this.
            raise OSError(-1, "CONNACK reason code 0x%x" % connack_resp[1])

        del connack_resp
        if not mqttv5:
            # If we are not on MQTTv5 we can stop here
            return

        connack_props_length, _ = await self._recv_len()
        if connack_props_length > 0:
            connack_props = await self._as_read(connack_props_length)
            decoded_props = decode_properties(connack_props, connack_props_length)
            self.dprint("CONNACK properties: %s", decoded_props)
            self.topic_alias_maximum = decoded_props.get(0x22, 0)

    async def _ping(self):
        async with self.lock:
            await self._as_write(b"\xc0\0")

    # Check internet connectivity by sending DNS lookup to Google's 8.8.8.8
    async def wan_ok(
        self,
        packet=b"$\x1a\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x03www\x06google\x03com\x00\x00\x01\x00\x01",
    ):
        if not self.isconnected():  # WiFi is down
            return False
        length = 32  # DNS query and response packet size
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setblocking(False)
        s.connect(("8.8.8.8", 53))
        await asyncio.sleep(1)
        async with self.lock:
            try:
                await self._as_write(packet, sock=s)
                await asyncio.sleep(2)
                res = await self._as_read(length, s)
                if len(res) == length:
                    return True  # DNS response size OK
            except OSError:  # Timeout on read: no connectivity.
                return False
            finally:
                s.close()
        return False

    async def broker_up(self):  # Test broker connectivity
        if not self.isconnected():
            return False
        tlast = self.last_rx
        if ticks_diff(ticks_ms(), tlast) < 1000:
            return True
        try:
            await self._ping()
        except OSError:
            return False
        t = ticks_ms()
        while not self._timeout(t):
            await asyncio.sleep_ms(100)
            if ticks_diff(self.last_rx, tlast) > 0:  # Response received
                return True
        return False

    async def disconnect(self):
        if self._sock is not None:
            await self._kill_tasks(False)  # Keep socket open
            try:
                async with self.lock:
                    self._sock.write(b"\xe0\0")  # Close broker connection
                    await asyncio.sleep_ms(100)
            except OSError:
                pass
            self._close()
        self._has_connected = False

    def _close(self):
        if self._sock is not None:
            self._sock.close()

    def close(self):  # API. See https://github.com/peterhinch/micropython-mqtt/issues/60
        self._close()
        if self._external_wifi:
            # PATCH local: WiFi externo e do light_mesh.py -- fechar o MQTT
            # nao deve derrubar a conexao usada por mesh/UI/RTC.
            return
        try:
            self._sta_if.disconnect()  # Disconnect Wi-Fi to avoid errors
        except OSError:
            self.dprint("Wi-Fi not started, unable to disconnect interface")
        self._sta_if.active(False)

    async def _await_pid(self, pid):
        t = ticks_ms()
        while pid in self.rcv_pids:  # local copy
            if self._timeout(t) or not self.isconnected():
                break  # Must repub or bail out
            await asyncio.sleep_ms(100)
        else:
            # PATCH local (2026-09-24): o pid saiu de rcv_pids tanto num
            # ack de sucesso quanto num NAK (ver kill_pid() chamado nos 2
            # casos, PUBACK/[UN]SUBACK em wait_msg()) -- sem este check,
            # um NAK (ex: ACL negada) virava falso-positivo aqui (achado
            # ao vivo testando um device sem autorizacao nenhuma no
            # broker: subscribe()/publish() reportavam sucesso pra uma
            # rejeicao de verdade). So devolve True se NAO foi NAK.
            #
            # PATCH local (2026-09-24, 2a rodada): num NAK devolve o proprio
            # reason code (int >= 0x80) em vez de False -- False continua
            # significando SO "sem resposta/desconectou" (quem chama faz
            # republish/reconnect). Um NAK e resposta definitiva do broker:
            # quem chama loga e avisa via _report_nak(), sem reconectar.
            if pid in self._nak_pids:
                return self._nak_pids.pop(pid)
            return True  # PID recebido e aceito. Tudo certo.
        return False

    # PATCH local (2026-09-24): negacao do broker (ACL, topico invalido...)
    # NAO e falha de conexao -- antes virava OSError, _handle_msg reconectava
    # e subscribe()/publish() retentavam pra sempre (cada tentativa = uma
    # reconexao completa; achado no teste P+N real com um topico sem ACL).
    # Decisao do Rafael: so loga e avisa (nak_cb publica no topico `error`).
    def _report_nak(self, kind, topic, reason_code):
        if isinstance(topic, (bytes, bytearray)):
            topic = bytes(topic).decode()
        print("mqtt_as: broker NEGOU %s em %s (reason code 0x%x)" % (kind, topic, reason_code))
        if self._nak_cb is not None:
            try:
                self._nak_cb(kind, topic, reason_code)
            except Exception as e:
                print("mqtt_as: nak_cb falhou:", repr(e))

    # qos == 1: coro blocks until wait_msg gets correct PID.
    # If WiFi fails completely subclass re-publishes with new PID.
    def _merge_pub_props(self, properties):
        base = self._pub_props
        if not base:
            return properties
        if not properties:
            return base
        out = dict(base)
        for key, val in properties.items():
            if key == 0x26 and 0x26 in base:
                user = dict(base[0x26])
                user.update(val)
                out[key] = user
            else:
                out[key] = val
        return out

    async def publish(self, topic, msg, retain, qos, properties=None):
        properties = self._merge_pub_props(properties)
        pid = next(self.newpid)
        if qos:
            self.rcv_pids.add(pid)
        async with self.lock:
            await self._publish(topic, msg, retain, qos, 0, pid, properties)
        if qos == 0:
            return

        count = 0
        while 1:  # Await PUBACK, republish on timeout
            res = await self._await_pid(pid)
            if res is True:
                return
            if res is not False:  # NAK: resposta definitiva, nao republica
                self._report_nak("PUBLISH", topic, res)
                return False
            # No match
            if count >= self._max_repubs or not self.isconnected():
                raise OSError(-1)  # Subclass to re-publish with new PID
            async with self.lock:
                # Add pid
                await self._publish(topic, msg, retain, qos, dup=1, pid=pid, properties=properties)
            count += 1
            self.REPUB_COUNT += 1

    async def _publish(self, topic, msg, retain, qos, dup, pid, properties=None):
        pkt = bytearray(b"\x30\0\0\0")
        pkt[0] |= qos << 1 | retain | dup << 3
        sz = 2 + len(topic) + len(msg)
        if qos > 0:
            sz += 2

        if self.mqttv5:
            properties = encode_properties(properties)
            sz += len(properties)

        await self._as_write(pkt, vbi(pkt, 1, sz))  # Encode size as VBI
        await self._send_str(topic)
        if qos > 0:
            struct.pack_into("!H", pkt, 0, pid)
            await self._as_write(pkt, 2)
        if self.mqttv5:
            await self._as_write(properties)
        await self._as_write(msg)

    # PATCH local (2026-09-24): devolve o resultado do _usub() -- False quando
    # o broker NEGA (SUBACK >= 0x80). Sem o `return`, o False se perdia aqui e
    # quem chamava nunca via a negacao (achado em HW: "inscrito" logado logo
    # depois de "broker NEGOU SUBSCRIBE").
    async def subscribe(self, topic, qos, properties=None):
        return await self._usub(topic, qos, properties)

    async def unsubscribe(self, topic, properties=None):
        return await self._usub(topic, None, properties)

    # Subscribe/unsubscribe
    # Can raise OSError if WiFi fails. Subclass traps.
    async def _usub(self, topic, qos, properties):
        sub = qos is not None
        pkt = bytearray(7)
        pkt[0] = 0x82 if sub else 0xA2
        pid = next(self.newpid)
        self.rcv_pids.add(pid)
        # 2 bytes of PID + 2 bytes of topic length + len(topic)
        sz = 2 + 2 + len(topic) + (1 if sub else 0)
        if self.mqttv5:
            # Return length as VBI followed by properties or b'\0'
            properties = encode_properties(properties)
            sz += len(properties)
        offs = vbi(pkt, 1, sz)  # Store size as variable byte integer
        struct.pack_into("!H", pkt, offs, pid)

        async with self.lock:
            await self._as_write(pkt, offs + 2)
            if self.mqttv5:
                await self._as_write(properties)
            await self._send_str(topic)
            if sub:
                # Only QoS is supported other features such as:
                # (NL) No Local, (RAP) Retain As Published and Retain Handling.
                # Are not supported.
                await self._as_write(qos.to_bytes(1, "little"))

        res = await self._await_pid(pid)
        if res is False:
            raise OSError(-1)
        if res is not True:  # NAK: resposta definitiva, nao reconecta
            self._report_nak("SUBSCRIBE" if sub else "UNSUBSCRIBE", topic, res)
            return False

    # Remove a pending pid after a successful receive.
    def kill_pid(self, pid, msg):
        if pid in self.rcv_pids:
            self.rcv_pids.discard(pid)
        else:
            raise OSError(-1, f"Invalid pid in {msg} packet")

    # Wait for a single incoming MQTT message and process it.
    # Subscribed messages are delivered to a callback previously
    # set by .setup() method. Other (internal) MQTT
    # messages processed internally.
    # Immediate return if no data available. Called from ._handle_msg().
    async def wait_msg(self):
        mqttv5 = self.mqttv5  # Cache local
        try:
            res = self._sock.read(1)  # Throws OSError on WiFi fail
        except OSError as e:
            if e.args[0] in BUSY_ERRORS:  # Needed by RP2
                await asyncio.sleep_ms(0)
                return
            raise

        if res is None:
            return
        if res == b"":
            raise OSError(-1, "Empty response")  # Can happen on broker fail

        if res == b"\xd0":  # PINGRESP
            await self._as_read(1)  # Update .last_rx time
            return
        op = res[0]

        if op == 0x40:  # PUBACK
            sz, _ = await self._recv_len()
            if not mqttv5 and sz != 2:
                raise OSError(-1, "Invalid PUBACK packet")
            rcv_pid = await self._as_read(2)
            pid = rcv_pid[0] << 8 | rcv_pid[1]
            # For some reason even on MQTTv5 reason code is optional
            if sz != 2:
                reason_code = await self._as_read(1)
                reason_code = reason_code[0]
                if reason_code >= 0x80:
                    # PATCH local (2026-09-24): libera o pid pendente ANTES
                    # de levantar o erro -- sem isso, quem chamou publish()
                    # (_await_pid) fica esperando pra sempre um ack que
                    # nunca mais sera processado (esta task morre logo em
                    # seguida, capturada por _handle_msg's `except OSError`,
                    # que so faz `_reconnect()` sem limpar rcv_pids). Achado
                    # ao vivo testando um device SEM autorizacao nenhuma no
                    # broker (PUBACK/SUBACK negados travando o client pra
                    # sempre em vez de erro limpo), 2026-09-24. Registra em
                    # _nak_pids ANTES de kill_pid() -- sem isso, _await_pid()
                    # nao consegue diferenciar "ack de sucesso" de "nak" so
                    # olhando rcv_pids, e falso-positiva um NAK como sucesso
                    # (2o bug achado no mesmo dia, revisao do agente do
                    # servidor).
                    #
                    # 2a rodada (2026-09-24): NAO levanta mais -- o NAK
                    # chega em publish() via _await_pid() (reason code) e
                    # vira log + nak_cb, sem derrubar a conexao.
                    self._nak_pids[pid] = reason_code
            if sz > 3:
                puback_props_sz, _ = await self._recv_len()
                if puback_props_sz > 0:
                    puback_props = await self._as_read(puback_props_sz)
                    decoded_props = decode_properties(puback_props, puback_props_sz)
                    self.dprint("PUBACK properties %s", decoded_props)
            # No exception thrown: PUBACK successfuly received. Remove pending PID
            self.kill_pid(pid, "PUBACK")

        if op == 0x90 or op == 0xB0:  # [UN]SUBACK
            un = "UN" if op == 0xB0 else ""
            suback = op == 0x90
            sz, _ = await self._recv_len()
            rcv_pid = await self._as_read(2)
            pid = rcv_pid[0] << 8 | rcv_pid[1]
            sz -= 2
            # Handle properties
            if mqttv5:
                suback_props_sz, sz_len = await self._recv_len()
                sz -= sz_len
                sz -= suback_props_sz
                if suback_props_sz > 0:
                    suback_props = await self._as_read(suback_props_sz)
                    decoded_props = decode_properties(suback_props, suback_props_sz)
                    self.dprint("[UN] SUBACK properties %s", decoded_props)

            if sz > 1:
                raise OSError(-1, "Got too many bytes")
            if suback or mqttv5:
                reason_code = await self._as_read(sz)
                reason_code = reason_code[0]
                if reason_code >= 0x80:
                    # PATCH local (2026-09-24): mesmo racional do PUBACK
                    # acima -- libera o pid ANTES de levantar o erro, senao
                    # subscribe()/unsubscribe() ficam travados pra sempre.
                    # _nak_pids ANTES de kill_pid() -- mesmo motivo do PUBACK
                    # (ver comentario la em cima): sem isso, _await_pid()
                    # falso-positiva o NAK como sucesso.
                    # 2a rodada (2026-09-24): NAO levanta mais -- mesmo
                    # racional do PUBACK acima (vira log + nak_cb em _usub()).
                    self._nak_pids[pid] = reason_code
            self.kill_pid(pid, f"{un}SUBACK")

        if op == 0xE0:  # DISCONNECT
            if mqttv5:
                sz, _ = await self._recv_len()
                reason_code = await self._as_read(1)
                reason_code = reason_code[0]

                sz -= 1
                if sz > 0:
                    dis_props_sz, dis_len = await self._recv_len()
                    sz -= dis_len
                    disconnect_props = await self._as_read(dis_props_sz)
                    decoded_props = decode_properties(disconnect_props, dis_props_sz)
                    self.dprint("DISCONNECT properties %s", decoded_props)

                if reason_code >= 0x80:
                    raise OSError(-1, "DISCONNECT reason code 0x%x" % reason_code)

        if op & 0xF0 != 0x30:
            return

        sz, _ = await self._recv_len()
        topic_len = await self._as_read(2)
        topic_len = (topic_len[0] << 8) | topic_len[1]
        topic = await self._as_read(topic_len)
        topic = bytes(topic)  # Copy before re-using the read buffer
        sz -= topic_len + 2
        # MQTT V3.1.1 section 2.3.1 non-normative comment. Get server PID.
        if op & 6:  # This is distinct from client PIDs.
            pid = await self._as_read(2)
            pid = pid[0] << 8 | pid[1]
            sz -= 2

        decoded_props = None
        if mqttv5:
            pub_props_sz, pub_props_sz_len = await self._recv_len()
            sz -= pub_props_sz_len
            sz -= pub_props_sz
            if pub_props_sz > 0:
                pub_props = await self._as_read(pub_props_sz)
                decoded_props = decode_properties(pub_props, pub_props_sz)

        msg = await self._as_read(sz)
        # In event mode we must copy the message otherwise .queue contents will be wrong:
        # every entry would contain the same message.
        # In callback mode not copying the message is OK so long as the callback is purely
        # synchronous. Overruns can't occur because of the lock.
        if self._events or MSG_BYTES:
            msg = bytes(msg)
        retained = op & 0x01
        args = [topic, msg, bool(retained)]
        if mqttv5:
            args.append(decoded_props)
        self._cb(*args)

        if op & 6 == 2:  # qos 1
            pkt = bytearray(b"\x40\x02\0\0")  # Send PUBACK
            struct.pack_into("!H", pkt, 2, pid)
            await self._as_write(pkt)
        elif op & 6 == 4:  # qos 2 not supported
            raise OSError(-1, "QoS 2 not supported")


# MQTTClient class. Handles issues relating to connectivity.


class MQTTClient(MQTT_base):
    def __init__(self, config):
        super().__init__(config)
        self._isconnected = False  # Current connection state
        keepalive = 1000 * self._keepalive  # ms
        self._ping_interval = keepalive // 4 if keepalive else 20000
        p_i = config["ping_interval"] * 1000  # Can specify shorter e.g. for subscribe-only
        if p_i and p_i < self._ping_interval:
            self._ping_interval = p_i
        self._in_connect = False
        self._has_connected = False  # Define 'Clean Session' value to use.
        self._tasks = []
        if ESP8266:
            import esp

            esp.sleep_type(0)  # Improve connection integrity at cost of power consumption.

    async def wifi_connect(self, quick=False):
        s = self._sta_if
        if self._external_wifi:
            # PATCH local (2026-08-26): mqtt_as e quem cuida de conectar/
            # reconectar tambem na interface externa (network.ESP_HOSTED()
            # deste projeto) -- igual faria com WLAN nativo, via
            # self._ssid/self._wifi_pw (config["ssid"]/["wifi_pw"]). Versao
            # anterior so ESPERAVA (s.isconnected()) sem nunca chamar
            # .connect() sozinho, dependendo de outro modulo (light_mesh.py)
            # pra trazer a interface de volta -- bug real achado por Rafael
            # derrubando o roteador de proposito: a interface nunca
            # reconectava sozinha. Nao chamamos .active()/.disconnect() aqui
            # (quem cria/liga a interface e o dono dela fora deste modulo).
            if not s.isconnected():
                if self._ssid is None or self._wifi_pw is None:
                    raise OSError("Wi-Fi externo sem ssid/wifi_pw configurados (config)")
                try:
                    s.connect(self._ssid, self._wifi_pw)
                except OSError:
                    pass  # ja pode estar tentando conectar -- segue pro poll abaixo
            for _ in range(60):
                if s.isconnected():
                    return
                await asyncio.sleep(1)
            raise OSError("Wi-Fi externo nao conectou a tempo")
        if ESP8266:
            if s.isconnected():  # 1st attempt, already connected.
                return
            s.active(True)
            s.connect()  # ESP8266 remembers connection.
            if 'ifconfig' in config:
                s.ifconfig(config['ifconfig'])

            for _ in range(60):
                # Break out on fail or success. Check once per sec.
                if s.status() != network.STAT_CONNECTING:
                    break
                await asyncio.sleep(1)
            # might hang forever awaiting dhcp lease renewal or something else
            if s.status() == network.STAT_CONNECTING:
                s.disconnect()
                await asyncio.sleep(1)
            if not s.isconnected() and self._ssid is not None and self._wifi_pw is not None:
                s.connect(self._ssid, self._wifi_pw)
                if 'ifconfig' in config:
                    s.ifconfig(config['ifconfig'])

                # Break out on fail or success. Check once per sec.
                while s.status() == network.STAT_CONNECTING:
                    await asyncio.sleep(1)
        else:
            s.active(True)
            if RP2 and not NINA:  # Disable auto-sleep.
                # https://datasheets.raspberrypi.com/picow/connecting-to-the-internet-with-pico-w.pdf
                # para 3.6.3
                s.config(pm=0xA11140)
            s.connect(self._ssid, self._wifi_pw)
            if 'ifconfig' in config:
                s.ifconfig(config['ifconfig'])
            for _ in range(60):  # Break out on fail or success. Check once per sec.
                await asyncio.sleep(1)
                # Loop while connecting or no IP
                if s.isconnected():
                    break
                if ESP32:
                    # Status values >= STAT_IDLE can occur during connect:
                    # STAT_IDLE 1000, STAT_CONNECTING 1001, STAT_GOT_IP 1010
                    # Error statuses are in range 200..204
                    if s.status() < network.STAT_IDLE:
                        # pause as workaround to avoid persistent reconnect failures
                        # see https://github.com/peterhinch/micropython-mqtt/issues/132 for details
                        await asyncio.sleep(1)
                        break
                elif PYBOARD:  # No symbolic constants in network
                    if not 1 <= s.status() <= 2:
                        break
                elif RP2 and not NINA:  # 1 is STAT_CONNECTING. 2 reported by user (No IP?)
                    if not 1 <= s.status() <= 2:
                        break
            else:  # Timeout: still in connecting state
                s.disconnect()
                await asyncio.sleep(1)

        if not s.isconnected():  # Timed out
            raise OSError("Wi-Fi connect timed out")
        if not quick:  # Skip on first connection only if power saving
            # Ensure connection stays up for a few secs.
            self.dprint("Checking WiFi integrity.")
            for _ in range(5):
                if not s.isconnected():
                    raise OSError("Connection Unstable")  # in 1st 5 secs
                await asyncio.sleep(1)
            self.dprint("Got reliable connection")

    async def connect(self, *, quick=False):  # Quick initial connect option for battery apps
        if not self._has_connected:
            await self.wifi_connect(quick)  # On 1st call, caller handles error
            # Note this blocks if DNS lookup occurs. Do it once to prevent
            # blocking during later internet outage:
            self._addr = socket.getaddrinfo(self.server, self.port)[0][-1]
        self._in_connect = True  # Disable low level ._isconnected check
        try:
            is_clean = self._clean
            if not self._has_connected and self._clean_init and not self._clean:
                if self.mqttv5:
                    is_clean = True
                else:
                    # Power up. Clear previous session data but subsequently save it.
                    # Issue #40
                    await self._connect(True)  # Connect with clean session
                    try:
                        async with self.lock:
                            self._sock.write(b"\xe0\0")  # Force disconnect but keep socket open
                    except OSError:
                        pass
                    self.dprint("Waiting for disconnect")
                    await asyncio.sleep(2)  # Wait for broker to disconnect
                    self.dprint("About to reconnect with unclean session.")
            await self._connect(is_clean)
        except Exception:
            self._close()
            self._in_connect = False  # Caller may run .isconnected()
            raise
        self.rcv_pids.clear()
        self._nak_pids.clear()  # PATCH local (2026-09-24): sessao nova, sem NAK obsoleto
        # If we get here without error broker/LAN must be up.
        self._isconnected = True
        self._in_connect = False  # Low level code can now check connectivity.
        if not self._events:
            asyncio.create_task(self._wifi_handler(True))  # User handler.
        if not self._has_connected:
            self._has_connected = True  # Use normal clean flag on reconnect.
            asyncio.create_task(self._keep_connected())
            # Runs forever unless user issues .disconnect()

        asyncio.create_task(self._handle_msg())  # Task quits on connection fail.
        self._tasks.append(asyncio.create_task(self._keep_alive()))
        if self.DEBUG:
            self._tasks.append(asyncio.create_task(self._memory()))
        if self._events:
            self.up.set()  # Connectivity is up
        else:
            asyncio.create_task(self._connect_handler(self))  # User handler.

    # Launched by .connect(). Runs until connectivity fails. Checks for and
    # handles incoming messages.
    async def _handle_msg(self):
        try:
            while self.isconnected():
                async with self.lock:
                    await self.wait_msg()  # Immediate return if no message
                # https://github.com/peterhinch/micropython-mqtt/issues/166
                # A delay > 0 is necessary for webrepl compatibility.
                await asyncio.sleep_ms(5)  # Let other tasks get lock

        except OSError:
            pass
        self._reconnect()  # Broker or WiFi fail.

    # Keep broker alive MQTT spec 3.1.2.10 Keep Alive.
    # Runs until ping failure or no response in keepalive period.
    async def _keep_alive(self):
        while self.isconnected():
            pings_due = ticks_diff(ticks_ms(), self.last_rx) // self._ping_interval
            if pings_due >= 4:
                self.dprint("Reconnect: broker fail.")
                break
            await asyncio.sleep_ms(self._ping_interval)
            try:
                await self._ping()
            except OSError:
                break
        self._reconnect()  # Broker or WiFi fail.

    async def _kill_tasks(self, kill_skt):  # Cancel running tasks
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        await asyncio.sleep_ms(0)  # Ensure cancellation complete
        if kill_skt:  # Close socket
            self._close()

    # DEBUG: show RAM messages.
    async def _memory(self):
        while True:
            await asyncio.sleep(20)
            gc.collect()
            self.dprint("RAM free %d alloc %d", gc.mem_free(), gc.mem_alloc())

    def isconnected(self):
        if self._in_connect:  # Disable low-level check during .connect()
            return True

        if self._isconnected and not self._sta_if.isconnected():  # It's going down.
            self._reconnect()
        return self._isconnected

    def _reconnect(self):  # Schedule a reconnection if not underway.
        if self._isconnected:
            self._isconnected = False
            asyncio.create_task(self._kill_tasks(True))  # Shut down tasks and socket
            if self._events:  # Signal an outage
                self.down.set()
            else:
                asyncio.create_task(self._wifi_handler(False))  # User handler.

    # Await broker connection.
    async def _connection(self):
        while not self._isconnected:
            await asyncio.sleep(1)

    # Scheduled on 1st successful connection. Runs forever maintaining wifi and
    # broker connection. Must handle conditions at edge of WiFi range.
    async def _keep_connected(self):
        while self._has_connected:
            if self.isconnected():  # Pause for 1 second
                await asyncio.sleep(1)
                gc.collect()
            else:  # Link is down, socket is closed, tasks are killed
                if not self._external_wifi:
                    try:
                        self._sta_if.disconnect()
                    except OSError:
                        self.dprint("Wi-Fi not started, unable to disconnect interface")
                # PATCH local: com WiFi externo, nao desconecta -- so espera
                # o light_mesh.py trazer a interface de volta sozinho.
                await asyncio.sleep(1)
                try:
                    await self.wifi_connect()
                except OSError:
                    continue
                if not self._has_connected:  # User has issued the terminal .disconnect()
                    self.dprint("Disconnected, exiting _keep_connected")
                    break
                try:
                    await self.connect()
                    # Now has set ._isconnected and scheduled _connect_handler().
                    self.dprint("Reconnect OK!")
                except OSError as e:
                    self.dprint("Error in reconnect. %s", e)
                    # Can get ECONNABORTED or -1. The latter signifies no or bad CONNACK received.
                    self._close()  # Disconnect and try again.
                    self._in_connect = False
                    self._isconnected = False
        self.dprint("Disconnected, exited _keep_connected")

    async def subscribe(self, topic, qos=0, properties=None):
        qos_check(qos)
        while 1:
            await self._connection()
            try:
                return await super().subscribe(topic, qos, properties)
            except OSError:
                pass
            self._reconnect()  # Broker or WiFi fail.

    async def unsubscribe(self, topic, properties=None):
        while 1:
            await self._connection()
            try:
                return await super().unsubscribe(topic, properties)
            except OSError:
                pass
            self._reconnect()  # Broker or WiFi fail.

    async def publish(self, topic, msg, retain=False, qos=0, properties=None):
        qos_check(qos)
        while 1:
            await self._connection()
            try:
                return await super().publish(topic, msg, retain, qos, properties)
            except OSError:
                pass
            self._reconnect()  # Broker or WiFi fail.
