import json
import os
import smtplib
import time
import uuid
import threading
from email.message import EmailMessage
from functools import wraps
from flask import Flask, request, redirect, url_for, session, jsonify, render_template_string
import paho.mqtt.client as mqtt

app = Flask(__name__)
app.secret_key = "clave-secreta-redes-ur-iot"

# ==========================================
# 1. CREDENCIALES DE ACCESO (Requerimiento 5)
# ==========================================
USUARIOS_VALIDOS = {
    "admin": "redes2026",
    "profesor": "iot1234"
}

# ==========================================
# 2. CONFIGURACIÓN MQTT Y TIMEOUT (5s)
# ==========================================
MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883
TOPIC_COMANDO = "ur/redes/iot/luz/comando"
TOPIC_CONFIRMACION = "ur/redes/iot/luz/confirmacion"
TOPIC_ALERTA = "ur/redes/iot/movimiento/alerta"
TIMEOUT_SEGUNDOS = 5.0
COOLDOWN_ALERTA_SEGUNDOS = 30.0

# ==========================================
# 2B. CONFIGURACIÓN SMTP GMAIL (Requerimiento 4)
# ==========================================
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_USER = os.environ.get("SMTP_USER", "").strip()
SMTP_PASS = os.environ.get("SMTP_PASS", "").strip()
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "").strip() or SMTP_USER
SMTP_CONFIGURADO = bool(SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO)

if not SMTP_CONFIGURADO:
    print(
        "[SMTP][AVISO] Variables SMTP_USER y/o SMTP_PASS no configuradas. "
        "Las alertas de movimiento se registrarán en consola y panel web, pero no se enviarán correos."
    )

lock_estado = threading.Lock()
comandos_pendientes = {}  # { id_accion: {"timestamp": float, "accion": str} }
ultima_alerta_enviada_ts = 0.0

estado_sistema = {
    "luz_encendida": False,
    "intensidad_actual": 0,
    "estado_transaccion": "IDLE",  # IDLE | PENDING | OK | ERROR | TIMEOUT
    "mensaje_transaccion": "Sistema listo. Esperando acción...",
    "ultima_confirmacion": "Ninguna aún.",
    "historial": [],
    "ultima_alerta_movimiento": {
        "detectada": False,
        "timestamp": None,
        "evento": None,
        "sensor": None,
        "origen": None,
        "estado_correo": "SIN_ALERTAS",  # SIN_ALERTAS | ENVIANDO | ENVIADO | ERROR | NO_CONFIGURADO
        "detalle_correo": "Sin detecciones de movimiento registradas aún.",
        "payload_raw": None
    }
}

def registrar_historial(tipo, detalle):
    estado_sistema["historial"].insert(0, {
        "hora": time.strftime("%H:%M:%S"),
        "tipo": tipo,
        "detalle": detalle
    })
    estado_sistema["historial"] = estado_sistema["historial"][:12]

def enviar_correo_alerta_async(evento, sensor, origen, timestamp_str, payload_str):
    try:
        msg = EmailMessage()
        msg["Subject"] = f"[Alerta IoT] Movimiento detectado ({sensor})"
        msg["From"] = SMTP_USER
        msg["To"] = ALERT_EMAIL_TO
        msg.set_content(
            "Se ha detectado una alerta de movimiento en el sistema IoT.\n\n"
            f"- Evento: {evento}\n"
            f"- Sensor: {sensor}\n"
            f"- Origen: {origen}\n"
            f"- Fecha y hora: {timestamp_str}\n"
            f"- Tópico MQTT: {TOPIC_ALERTA}\n"
            f"- Payload recibido: {payload_str}\n"
        )

        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(msg)

        with lock_estado:
            estado_sistema["ultima_alerta_movimiento"]["estado_correo"] = "ENVIADO"
            estado_sistema["ultima_alerta_movimiento"]["detalle_correo"] = (
                f"Correo enviado a {ALERT_EMAIL_TO} ({timestamp_str})"
            )
            registrar_historial("MAIL_OK", f"Correo enviado a {ALERT_EMAIL_TO}")
        print(f"[SMTP] Correo de alerta enviado a {ALERT_EMAIL_TO}")
    except Exception as exc:
        with lock_estado:
            estado_sistema["ultima_alerta_movimiento"]["estado_correo"] = "ERROR"
            estado_sistema["ultima_alerta_movimiento"]["detalle_correo"] = (
                f"Error al enviar correo: {exc}"
            )
            registrar_historial("MAIL_ERR", f"Error SMTP: {exc}")
        print(f"[SMTP][ERROR] No se pudo enviar el correo de alerta: {exc}")

def procesar_confirmacion_luz(payload_str):
    try:
        data = json.loads(payload_str)
        id_resp = data.get("id_accion", "")
        estado_ack = data.get("estado", "DESCONOCIDO")

        with lock_estado:
            # Verificar si corresponde a un comando pendiente dentro del tiempo límite
            info_pendiente = comandos_pendientes.pop(id_resp, None)
            latencia_ms = None
            if info_pendiente:
                latencia_ms = round((time.time() - info_pendiente["timestamp"]) * 1000)

            estado_sistema["luz_encendida"] = data.get("luz_encendida", False)
            estado_sistema["intensidad_actual"] = data.get("intensidad_actual", 0)
            estado_sistema["ultima_confirmacion"] = payload_str

            if estado_ack == "OK":
                estado_sistema["estado_transaccion"] = "OK"
                extra = f" ({latencia_ms} ms)" if latencia_ms is not None else ""
                estado_sistema["mensaje_transaccion"] = f"✅ Confirmado [{id_resp}]{extra}"
                registrar_historial("ACK_OK", f"{payload_str}{extra}")
            else:
                estado_sistema["estado_transaccion"] = "ERROR"
                estado_sistema["mensaje_transaccion"] = f"⚠️ Nodo reportó error [{id_resp}]: {estado_ack}"
                registrar_historial("ACK_ERR", payload_str)

    except json.JSONDecodeError:
        print("[ERROR] Mensaje recibido no es JSON válido")

def procesar_alerta_movimiento(payload_str):
    global ultima_alerta_enviada_ts
    try:
        data = json.loads(payload_str)
    except json.JSONDecodeError:
        print("[ERROR] Mensaje de alerta recibido no es JSON válido")
        return

    evento = data.get("evento", "MOVIMIENTO")
    sensor = data.get("sensor", "PIR_HW416B")
    origen = data.get("origen", "ESP32")
    ahora = time.time()
    timestamp_str = time.strftime("%Y-%m-%d %H:%M:%S")
    lanzar_hilo = False

    with lock_estado:
        if ultima_alerta_enviada_ts > 0 and (ahora - ultima_alerta_enviada_ts) < COOLDOWN_ALERTA_SEGUNDOS:
            restante = round(COOLDOWN_ALERTA_SEGUNDOS - (ahora - ultima_alerta_enviada_ts), 1)
            msg_cd = f"Evento {evento} ({sensor}) omitido por cooldown de respaldo ({restante}s restantes)"
            print(f"[ALERTA PIR] {msg_cd}")
            registrar_historial("PIR_COOLDOWN", msg_cd)
            return

        ultima_alerta_enviada_ts = ahora
        smtp_ok = bool(SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO)

        if smtp_ok:
            estado_sistema["ultima_alerta_movimiento"] = {
                "detectada": True,
                "timestamp": timestamp_str,
                "evento": evento,
                "sensor": sensor,
                "origen": origen,
                "estado_correo": "ENVIANDO",
                "detalle_correo": f"Enviando notificación a {ALERT_EMAIL_TO}...",
                "payload_raw": payload_str
            }
            registrar_historial("PIR_ALERTA", f"{evento} ({sensor}) -> Enviando correo a {ALERT_EMAIL_TO}")
            lanzar_hilo = True
        else:
            estado_sistema["ultima_alerta_movimiento"] = {
                "detectada": True,
                "timestamp": timestamp_str,
                "evento": evento,
                "sensor": sensor,
                "origen": origen,
                "estado_correo": "NO_CONFIGURADO",
                "detalle_correo": "SMTP_USER / SMTP_PASS no configurados; correo no enviado.",
                "payload_raw": payload_str
            }
            registrar_historial("PIR_ALERTA", f"{evento} ({sensor}) -> Correo omitido (SMTP no configurado)")
            print("[SMTP][AVISO] Alerta recibida, pero falta configurar SMTP_USER/SMTP_PASS para enviar el correo.")

    if lanzar_hilo:
        threading.Thread(
            target=enviar_correo_alerta_async,
            args=(evento, sensor, origen, timestamp_str, payload_str),
            daemon=True
        ).start()

def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"[MQTT] Conectado al bróker (código: {reason_code})")
    client.subscribe(TOPIC_CONFIRMACION)
    client.subscribe(TOPIC_ALERTA)

def on_message(client, userdata, msg):
    payload_str = msg.payload.decode("utf-8", errors="replace")
    print(f"[MQTT RX <- {msg.topic}] {payload_str}")
    if msg.topic == TOPIC_CONFIRMACION:
        procesar_confirmacion_luz(payload_str)
    elif msg.topic == TOPIC_ALERTA:
        procesar_alerta_movimiento(payload_str)

# Hilo vigilante que verifica comandos que superen los 5 segundos sin ACK
def vigilante_timeouts():
    while True:
        time.sleep(0.25)
        ahora = time.time()
        with lock_estado:
            expirados = [
                cid for cid, info in comandos_pendientes.items()
                if (ahora - info["timestamp"]) > TIMEOUT_SEGUNDOS
            ]
            for cid in expirados:
                info = comandos_pendientes.pop(cid)
                msg_timeout = f"❌ TIMEOUT (>{int(TIMEOUT_SEGUNDOS)}s): El nodo no confirmó [{cid}] ({info['accion']})"
                print(f"[TIMEOUT] {msg_timeout}")
                estado_sistema["estado_transaccion"] = "TIMEOUT"
                estado_sistema["mensaje_transaccion"] = msg_timeout
                registrar_historial("TIMEOUT", msg_timeout)

threading.Thread(target=vigilante_timeouts, daemon=True).start()

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

# ==========================================
# 3. PLANTILLAS HTML (LOGIN Y DASHBOARD)
# ==========================================
HTML_LOGIN = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <title>Login - Control IoT</title>
  <style>
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
    .card { background: #1e293b; padding: 2rem; border-radius: 12px; width: 320px; box-shadow: 0 10px 25px rgba(0,0,0,0.4); }
    h2 { margin-top: 0; text-align: center; color: #38bdf8; }
    input { width: 100%; padding: 10px; margin: 8px 0 16px; border-radius: 6px; border: 1px solid #334155; background: #0f172a; color: white; box-sizing: border-box; }
    button { width: 100%; padding: 10px; background: #0284c7; border: none; border-radius: 6px; color: white; font-weight: bold; cursor: pointer; }
    button:hover { background: #0369a1; }
    .error { color: #f87171; font-size: 0.9rem; text-align: center; margin-bottom: 10px; }
  </style>
</head>
<body>
  <div class="card">
    <h2>Plataforma IoT</h2>
    {% if error %}<div class="error">{{ error }}</div>{% endif %}
    <form method="POST">
      <label>Usuario</label>
      <input type="text" name="usuario" required placeholder="admin">
      <label>Contraseña</label>
      <input type="password" name="password" required placeholder="••••••••">
      <button type="submit">Ingresar</button>
    </form>
  </div>
</body>
</html>
"""

HTML_DASHBOARD = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <title>Panel de Iluminación IoT</title>
  <style>
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; max-width: 680px; margin: 40px auto; padding: 0 20px; }
    .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 15px; }
    .card { background: #1e293b; padding: 20px; border-radius: 12px; margin-top: 20px; }
    .btn-group { display: flex; gap: 10px; margin-bottom: 20px; }
    button { padding: 10px 18px; border: none; border-radius: 6px; font-weight: bold; cursor: pointer; color: white; }
    .btn-on { background: #16a34a; }
    .btn-off { background: #dc2626; }
    .btn-logout { background: #475569; text-decoration: none; padding: 8px 14px; border-radius: 6px; color: white; font-size: 0.9rem; }
    input[type="range"] { width: 100%; margin: 15px 0; }
    pre { background: #0f172a; padding: 12px; border-radius: 8px; overflow-x: auto; color: #38bdf8; font-size: 0.85rem; }
    .badge { display: inline-block; padding: 4px 10px; border-radius: 99px; font-size: 0.85rem; background: #334155; }
    .alerta-tx { padding: 12px; border-radius: 8px; font-weight: 600; margin-bottom: 15px; background: #334155; }
    .tx-IDLE { background: #334155; color: #cbd5e1; }
    .tx-PENDING { background: #854d0e; color: #fef08a; border: 1px solid #eab308; }
    .tx-OK { background: #14532d; color: #bbf7d0; border: 1px solid #22c55e; }
    .tx-ERROR, .tx-TIMEOUT { background: #7f1d1d; color: #fecaca; border: 1px solid #ef4444; }
    .pir-SIN_ALERTAS { background: #334155; color: #cbd5e1; }
    .pir-ENVIANDO { background: #854d0e; color: #fef08a; border: 1px solid #eab308; }
    .pir-NO_CONFIGURADO { background: #78350f; color: #fde68a; border: 1px solid #f59e0b; }
    .pir-ENVIADO { background: #14532d; color: #bbf7d0; border: 1px solid #22c55e; }
    .pir-ERROR { background: #7f1d1d; color: #fecaca; border: 1px solid #ef4444; }
  </style>
</head>
<body>
  <div class="header">
    <div>
      <h2 style="margin:0;">Control de Iluminación Remota</h2>
      <small style="color:#94a3b8;">Usuario activo: {{ usuario }} | Timeout ACK: 5.0 s</small>
    </div>
    <a href="/logout" class="btn-logout">Cerrar sesión</a>
  </div>

  <div class="card">
    <h3>1. Acciones de Iluminación (GPIO 12)</h3>
    <div class="btn-group">
      <button class="btn-on" onclick="enviarComando('ON')">Encender (100%)</button>
      <button class="btn-off" onclick="enviarComando('OFF')">Apagar (0%)</button>
    </div>

    <label>Intensidad PWM: <strong id="valSlider">0</strong>%</label>
    <input type="range" id="slider" min="0" max="100" value="0"
           oninput="document.getElementById('valSlider').innerText = this.value"
           onchange="enviarComando('DIM', this.value)">
  </div>

  <div class="card">
    <h3>2. Confirmación del Nodo ESP32 (ACK JSON + Timeout 5s)</h3>
    <div id="bannerTx" class="alerta-tx tx-IDLE">Sistema listo. Esperando acción...</div>
    <p>Estado real reportado: <span class="badge" id="estadoBadge">Desconocido</span></p>
    <pre id="ultimoJson">Ninguna aún.</pre>
    <h4>Historial de transacciones (ACK / Timeout):</h4>
    <pre id="historialJson">Sin registros aún.</pre>
  </div>

  <div class="card">
    <h3>3. Alerta de Movimiento ultrasónico (GPIO 27 + Correo)</h3>
    <div id="bannerPir" class="alerta-tx pir-SIN_ALERTAS">Sin detecciones de movimiento registradas aún.</div>
    <p>
      Última detección: <span class="badge" id="pirTimestamp">Ninguna</span>
      &nbsp;|&nbsp;
      Estado correo: <span class="badge" id="pirMailBadge">SIN_ALERTAS</span>
    </p>
    <pre id="pirDetalle">Esperando eventos en tópico ur/redes/iot/movimiento/alerta...</pre>
  </div>

  <script>
    async function enviarComando(accion, intensidad = null) {
      const payload = { accion: accion };
      if (intensidad !== null) payload.intensidad = parseInt(intensidad);

      await fetch('/api/luz', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      actualizarEstado();
    }

    async function actualizarEstado() {
      const res = await fetch('/api/estado');
      if (!res.ok) return;
      const data = await res.json();

      const banner = document.getElementById('bannerTx');
      banner.className = 'alerta-tx tx-' + data.estado_transaccion;
      banner.innerText = data.mensaje_transaccion;

      document.getElementById('estadoBadge').innerText =
        data.luz_encendida ? `ENCENDIDA (${data.intensidad_actual}%)` : 'APAGADA (0%)';
      document.getElementById('ultimoJson').innerText = data.ultima_confirmacion;
      document.getElementById('historialJson').innerText =
        data.historial.map(h => `[${h.hora}] [${h.tipo}] ${h.detalle}`).join('\\n') || 'Sin registros aún.';

      if (data.ultima_alerta_movimiento) {
        const alerta = data.ultima_alerta_movimiento;
        const bannerPir = document.getElementById('bannerPir');
        bannerPir.className = 'alerta-tx pir-' + (alerta.estado_correo || 'SIN_ALERTAS');
        bannerPir.innerText = alerta.detectada
          ? `Movimiento detectado (${alerta.sensor || 'PIR_HW416B'}) — ${alerta.detalle_correo}`
          : alerta.detalle_correo;
        document.getElementById('pirTimestamp').innerText = alerta.timestamp || 'Ninguna';
        document.getElementById('pirMailBadge').innerText = alerta.estado_correo || 'SIN_ALERTAS';
        document.getElementById('pirDetalle').innerText =
          alerta.payload_raw || 'Esperando eventos en tópico ur/redes/iot/movimiento/alerta...';
      }
    }

    setInterval(actualizarEstado, 500);
    actualizarEstado();
  </script>
</body>
</html>
"""

# ==========================================
# 4. RUTAS Y PROTECCIÓN DE SESIÓN
# ==========================================
def login_requerido(f):
    @wraps(f)
    def decorador(*args, **kwargs):
        if "usuario" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorador

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        usuario = request.form.get("usuario", "")
        password = request.form.get("password", "")
        if USUARIOS_VALIDOS.get(usuario) == password:
            session["usuario"] = usuario
            return redirect(url_for("dashboard"))
        error = "Credenciales inválidas."
    return render_template_string(HTML_LOGIN, error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/")
@login_requerido
def dashboard():
    return render_template_string(HTML_DASHBOARD, usuario=session["usuario"])

@app.route("/api/luz", methods=["POST"])
@login_requerido
def controlar_luz():
    datos = request.get_json()
    id_accion = f"web-{str(uuid.uuid4())[:6]}"
    comando_mqtt = {
        "id_accion": id_accion,
        "accion": datos.get("accion")
    }
    if "intensidad" in datos:
        comando_mqtt["intensidad"] = datos["intensidad"]

    with lock_estado:
        comandos_pendientes[id_accion] = {
            "timestamp": time.time(),
            "accion": comando_mqtt["accion"]
        }
        estado_sistema["estado_transaccion"] = "PENDING"
        estado_sistema["mensaje_transaccion"] = f"⏳ Esperando confirmación del nodo para [{id_accion}]..."

    mensaje_json = json.dumps(comando_mqtt)
    mqtt_client.publish(TOPIC_COMANDO, mensaje_json)
    print(f"[MQTT TX -> {TOPIC_COMANDO}] {mensaje_json}")
    return jsonify({"enviado": True, "comando": comando_mqtt})

@app.route("/api/estado")
@login_requerido
def obtener_estado():
    with lock_estado:
        return jsonify(estado_sistema)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
