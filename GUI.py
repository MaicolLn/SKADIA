# gui_modbus.py
from __future__ import annotations
import json
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path

# Funciones de tu módulo
from tool_f.modbus_tools import leer_medidor, escribir_plc, leer_plc

# --- Config por defecto ---
RUTA_JSON_DEF = "mapaIEEE13.json"
HOST_DEF, PORT_DEF = "192.168.1.200", 502


class ModbusGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Modbus/TCP – Medidores y PLC")
        self.geometry("760x520")
        self.minsize(720, 500)

        # tema/estilo
        self._setup_style()

        self.cfg: dict | None = None  # JSON cargado en memoria

        # UI
        self._build_topbar()
        self._build_notebook()
        self._build_output()

        # Carga inicial
        self._load_json()

    # --------------- Estilo -----------------
    def _setup_style(self):
        style = ttk.Style(self)
        # usar un tema moderno
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("TLabel", font=("Segoe UI", 10))
        style.configure("TButton", font=("Segoe UI", 10), padding=6)
        style.configure("Header.TLabel", font=("Segoe UI Semibold", 11))
        style.configure("Title.TLabel", font=("Segoe UI Semibold", 12))
        style.configure("Status.TLabel", font=("Segoe UI", 9), foreground="#555")
        style.configure("Value.TLabel", font=("Segoe UI Semibold", 13))
        style.configure("Card.TLabelframe", padding=10)
        style.configure("Card.TLabelframe.Label", font=("Segoe UI Semibold", 10))
        style.map("TButton",
                  relief=[("pressed", "sunken"), ("active", "raised")])

        # color base leve
        self.configure(bg="#f7f7fb")

    # --------------- Top bar ----------------
    def _build_topbar(self):
        frm = ttk.LabelFrame(self, text="Configuración", style="Card.TLabelframe")
        frm.pack(fill="x", padx=12, pady=(12, 6))

        # ruta JSON
        ttk.Label(frm, text="Ruta JSON:").grid(row=0, column=0, sticky="w")
        self.ent_json = ttk.Entry(frm, width=46)
        self.ent_json.insert(0, RUTA_JSON_DEF)
        self.ent_json.grid(row=0, column=1, sticky="we", padx=8)
        ttk.Button(frm, text="Recargar", command=self._load_json).grid(row=0, column=2, padx=(0, 8))

        # host/puerto
        ttk.Label(frm, text="Host:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.ent_host = ttk.Entry(frm, width=20)
        self.ent_host.insert(0, HOST_DEF)
        self.ent_host.grid(row=1, column=1, sticky="w", padx=8, pady=(6, 0))

        ttk.Label(frm, text="Puerto:").grid(row=1, column=2, sticky="e", pady=(6, 0))
        self.ent_port = ttk.Entry(frm, width=8)
        self.ent_port.insert(0, str(PORT_DEF))
        self.ent_port.grid(row=1, column=3, sticky="w", pady=(6, 0))

        # estirar entrada JSON
        frm.columnconfigure(1, weight=1)

    # --------------- Notebook ---------------
    def _build_notebook(self):
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=12, pady=(6, 6))
        self.nb = nb

        # ----- Tab Medidores -----
        self.tab_med = ttk.Frame(nb)
        nb.add(self.tab_med, text="Medidores")

        card_m = ttk.LabelFrame(self.tab_med, text="Lectura de registro (Holding FC3)", style="Card.TLabelframe")
        card_m.pack(fill="x", padx=6, pady=8)

        ttk.Label(card_m, text="Medidor:").grid(row=0, column=0, sticky="w")
        self.cbo_medidor = ttk.Combobox(card_m, state="readonly", width=28)
        self.cbo_medidor.grid(row=0, column=1, sticky="w", padx=8, pady=4)

        ttk.Label(card_m, text="Parámetro:").grid(row=1, column=0, sticky="w")
        self.cbo_param = ttk.Combobox(card_m, state="readonly", width=40)
        self.cbo_param.grid(row=1, column=1, sticky="w", padx=8, pady=4)
        self.cbo_param.bind("<<ComboboxSelected>>", self._on_param_changed)

        ttk.Label(card_m, text="Unidad:").grid(row=1, column=2, sticky="e")
        self.var_param_unit = tk.StringVar(value="")
        ttk.Label(card_m, textvariable=self.var_param_unit, style="Header.TLabel").grid(row=1, column=3, sticky="w", padx=6)

        ttk.Label(card_m, text="Decode:").grid(row=2, column=0, sticky="w")
        self.cbo_decode = ttk.Combobox(card_m, state="readonly", width=12,
                                       values=["auto", "float32", "u16", "u32"])
        self.cbo_decode.set("auto")
        self.cbo_decode.grid(row=2, column=1, sticky="w", padx=8, pady=(4, 0))

        act_m = ttk.Frame(self.tab_med)
        act_m.pack(fill="x", padx=6, pady=(0, 8))
        self.btn_leer = ttk.Button(act_m, text="Leer registro", command=self._on_leer_click)
        self.btn_leer.pack(side="left")
        self.btn_probe = ttk.Button(act_m, text="Probar crudo (FC3)", command=self._on_probe_click)
        self.btn_probe.pack(side="left", padx=8)

        # ----- Tab PLC -----
        self.tab_plc = ttk.Frame(nb)
        nb.add(self.tab_plc, text="PLC (coils)")

        card_p = ttk.LabelFrame(self.tab_plc, text="Operación de coils", style="Card.TLabelframe")
        card_p.pack(fill="x", padx=6, pady=8)

        ttk.Label(card_p, text="Nodo:").grid(row=0, column=0, sticky="w")
        self.cbo_nodo = ttk.Combobox(card_p, state="readonly", width=28)
        self.cbo_nodo.grid(row=0, column=1, sticky="w", padx=8, pady=4)
        self.cbo_nodo.bind("<<ComboboxSelected>>", self._on_nodo_changed)

        ttk.Label(card_p, text="Señal:").grid(row=1, column=0, sticky="w")
        self.cbo_senal = ttk.Combobox(card_p, state="readonly", width=28)
        self.cbo_senal.grid(row=1, column=1, sticky="w", padx=8, pady=4)

        act_p = ttk.Frame(self.tab_plc)
        act_p.pack(fill="x", padx=6, pady=(0, 8))
        self.btn_on  = ttk.Button(act_p, text="ON (True)",  command=lambda: self._plc_write(True))
        self.btn_off = ttk.Button(act_p, text="OFF (False)", command=lambda: self._plc_write(False))
        self.btn_read_coil = ttk.Button(act_p, text="Leer estado", command=self._plc_read)
        self.btn_on.pack(side="left")
        self.btn_off.pack(side="left", padx=8)
        self.btn_read_coil.pack(side="left")

    # --------------- Salida / log ----------
    def _build_output(self):
        card_out = ttk.LabelFrame(self, text="Resultado", style="Card.TLabelframe")
        card_out.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        ttk.Label(card_out, text="Valor:", style="Header.TLabel").grid(row=0, column=0, sticky="w", padx=(2, 0))
        self.var_valor = tk.StringVar(value="—")
        ttk.Label(card_out, textvariable=self.var_valor, style="Value.TLabel").grid(row=0, column=1, sticky="w", padx=6)

        self.txt_log = tk.Text(card_out, height=10, wrap="word", relief="flat", bg="#ffffff")
        self.txt_log.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(6, 0))
        ysb = ttk.Scrollbar(card_out, orient="vertical", command=self.txt_log.yview)
        ysb.grid(row=1, column=2, sticky="ns", pady=(6, 0))
        self.txt_log.configure(yscrollcommand=ysb.set)

        # barra de estado
        self.var_status = tk.StringVar(value="Listo")
        ttk.Label(self, textvariable=self.var_status, style="Status.TLabel").pack(fill="x", padx=14, pady=(0, 8))

        card_out.rowconfigure(1, weight=1)
        card_out.columnconfigure(1, weight=1)

    # --------------- Carga JSON ------------
    def _load_json(self):
        ruta = self.ent_json.get().strip()
        try:
            with open(ruta, "r", encoding="utf-8") as f:
                self.cfg = json.load(f)
        except Exception as e:
            messagebox.showerror("Error al cargar JSON", f"No se pudo abrir '{ruta}':\n{e}")
            self.var_status.set("Error al cargar JSON")
            return

        # Medidores
        meds = sorted(self.cfg.get("Medidores", {}).keys())
        params = sorted(self.cfg.get("Registros_Medidor", {}).keys())
        self.cbo_medidor["values"] = meds
        self.cbo_param["values"] = params
        if meds and not self.cbo_medidor.get():
            self.cbo_medidor.set(meds[0])
        if params and not self.cbo_param.get():
            self.cbo_param.set(params[0])

        # PLC
        nodos = sorted(self.cfg.get("Nodos", {}).keys())
        self.cbo_nodo["values"] = nodos
        if nodos and not self.cbo_nodo.get():
            self.cbo_nodo.set(nodos[0])
            self._populate_signals(nodos[0])

        # unidad por parámetro
        self._update_param_unit()

        self._log(f"Cargado JSON: {ruta} | Medidores={len(meds)} | Parámetros={len(params)} | Nodos={len(nodos)}")
        self.var_status.set("JSON cargado")

    def _populate_signals(self, nodo: str):
        try:
            d = self.cfg["Nodos"][nodo]
        except Exception:
            self.cbo_senal["values"] = []
            self.cbo_senal.set("")
            return
        señales = sorted([k for k in d.keys() if k.lower() != "slave"])
        self.cbo_senal["values"] = señales
        if señales and not self.cbo_senal.get():
            self.cbo_senal.set(señales[0])

    def _on_nodo_changed(self, _ev=None):
        self._populate_signals(self.cbo_nodo.get().strip())

    def _on_param_changed(self, _ev=None):
        self._update_param_unit()

    def _update_param_unit(self):
        unit = ""
        if self.cfg:
            p = self.cbo_param.get().strip()
            entry = self.cfg.get("Registros_Medidor", {}).get(p)
            if isinstance(entry, dict):
                unit = entry.get("unit") or ""
        self.var_param_unit.set(unit)

    # --------------- Acciones Medidores ----
    def _on_leer_click(self):
        medidor = self.cbo_medidor.get().strip()
        param = self.cbo_param.get().strip()
        decode_sel = self.cbo_decode.get().strip()
        host = self.ent_host.get().strip() or HOST_DEF
        try:
            port = int(self.ent_port.get().strip() or PORT_DEF)
        except ValueError:
            messagebox.showerror("Puerto inválido", "El puerto debe ser un número.")
            return
        if not medidor or not param:
            messagebox.showwarning("Faltan datos", "Selecciona medidor y parámetro.")
            return

        self.btn_leer.config(state="disabled")
        self.var_valor.set("Leyendo…")
        self.var_status.set("Leyendo registro…")
        self._log(f"[LEER] medidor={medidor} param={param} decode={decode_sel} {host}:{port}")

        t = threading.Thread(
            target=self._leer_worker,
            args=(medidor, param, decode_sel, host, port, self.ent_json.get().strip()),
            daemon=True
        )
        t.start()

    def _leer_worker(self, medidor, param, decode_sel, host, port, ruta_json):
        try:
            decode_arg = None if decode_sel == "auto" else decode_sel
            valor, unidad = leer_medidor(
                medidor, param,
                ruta_json=ruta_json, host=host, port=port,
                decode=decode_arg, return_unit=True
            )
            try:
                txt_val = f"{float(valor):.4f}"
            except (TypeError, ValueError):
                txt_val = str(valor)
            self._set_valor_ok(f"{txt_val} {unidad or ''}".strip())
            self._log("[OK] Lectura correcta.\n")
            self.var_status.set("Lectura OK")
        except Exception as e:
            self._set_valor_err()
            self._log(f"[ERROR] {type(e).__name__}: {e}\n")
            self.var_status.set("Error en lectura")
        finally:
            self.btn_leer.config(state="normal")

    # Probar crudo (FC3) para diagnóstico simple
    def _on_probe_click(self):
        medidor = self.cbo_medidor.get().strip()
        param = self.cbo_param.get().strip()
        host = self.ent_host.get().strip() or HOST_DEF
        try:
            port = int(self.ent_port.get().strip() or PORT_DEF)
        except ValueError:
            messagebox.showerror("Puerto inválido", "El puerto debe ser un número.")
            return
        if not medidor or not param:
            messagebox.showwarning("Faltan datos", "Selecciona medidor y parámetro.")
            return

        self.btn_probe.config(state="disabled")
        self.var_valor.set("Probando…")
        self.var_status.set("Prueba cruda FC3…")

        t = threading.Thread(
            target=self._probe_worker,
            args=(medidor, param, host, port, self.ent_json.get().strip()),
            daemon=True
        )
        t.start()

    def _probe_worker(self, medidor, param, host, port, ruta_json):
        try:
            with open(ruta_json, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            slave = int(cfg["Medidores"][medidor]["slave"])
            entry = cfg["Registros_Medidor"][param]
            reg_1b = int(entry["addr"]) if isinstance(entry, dict) else int(entry)
            addr0 = reg_1b - 1

            from pymodbus.client import ModbusTcpClient as C
            c = C(host, port=port, timeout=3.0, strict=False)
            if not c.connect():
                raise RuntimeError("No conecta al servidor Modbus")
            try:
                rr = c.read_holding_registers(address=addr0, count=2, slave=slave)
            finally:
                c.close()

            if not rr or rr.isError():
                self._set_valor_err()
                self._log(f"[PROBE] Sin respuesta válida de U{slave} @ HR[{reg_1b}]..+1: {rr}")
                self.var_status.set("Sin respuesta")
            else:
                self._set_valor_ok(f"raw={rr.registers}")
                self._log(f"[PROBE] HR[{reg_1b}] raw: {rr.registers}")
                self.var_status.set("Probe OK")
        except Exception as e:
            self._set_valor_err()
            self._log(f"[PROBE ERROR] {type(e).__name__}: {e}")
            self.var_status.set("Error en probe")
        finally:
            self.btn_probe.config(state="normal")

    # --------------- Acciones PLC ----------
    def _plc_write(self, valor_bool: bool):
        nodo = self.cbo_nodo.get().strip()
        sen = self.cbo_senal.get().strip()
        host = self.ent_host.get().strip() or HOST_DEF
        try:
            port = int(self.ent_port.get().strip() or PORT_DEF)
        except ValueError:
            messagebox.showerror("Puerto inválido", "El puerto debe ser un número.")
            return
        if not nodo or not sen:
            messagebox.showwarning("Faltan datos", "Selecciona nodo y señal.")
            return

        # evitar señales con null
        entry = self.cfg.get("Nodos", {}).get(nodo, {})
        if entry.get(sen, 0) in (None, "null"):
            messagebox.showwarning("Señal sin dirección",
                                   f"{nodo}.{sen} no tiene dirección (null) en el JSON.")
            return

        self.btn_on.config(state="disabled")
        self.btn_off.config(state="disabled")
        self.var_valor.set("Escribiendo…")
        self.var_status.set("Escribiendo coil…")
        self._log(f"[PLC WRITE] {nodo}.{sen} = {valor_bool} @ {host}:{port}")

        t = threading.Thread(
            target=self._plc_write_worker,
            args=(nodo, sen, valor_bool, host, port, self.ent_json.get().strip()),
            daemon=True
        )
        t.start()

    def _plc_write_worker(self, nodo, sen, valor, host, port, ruta_json):
        try:
            ok = escribir_plc(nodo, sen, valor, ruta_json=ruta_json, host=host, port=port, verificar=True)
            self._set_valor_ok(f"{nodo}.{sen} → {valor} (ok={ok})")
            self._log("[OK] Escritura confirmada.\n")
            self.var_status.set("Escritura OK")
        except Exception as e:
            self._set_valor_err()
            self._log(f"[PLC WRITE ERROR] {type(e).__name__}: {e}\n")
            self.var_status.set("Error en escritura")
        finally:
            self.btn_on.config(state="normal")
            self.btn_off.config(state="normal")

    def _plc_read(self):
        nodo = self.cbo_nodo.get().strip()
        sen = self.cbo_senal.get().strip()
        host = self.ent_host.get().strip() or HOST_DEF
        try:
            port = int(self.ent_port.get().strip() or PORT_DEF)
        except ValueError:
            messagebox.showerror("Puerto inválido", "El puerto debe ser un número.")
            return
        if not nodo or not sen:
            messagebox.showwarning("Faltan datos", "Selecciona nodo y señal.")
            return

        # evitar señales con null
        entry = self.cfg.get("Nodos", {}).get(nodo, {})
        if entry.get(sen, 0) in (None, "null"):
            messagebox.showwarning("Señal sin dirección",
                                   f"{nodo}.{sen} no tiene dirección (null) en el JSON.")
            return

        self.btn_read_coil.config(state="disabled")
        self.var_valor.set("Leyendo coil…")
        self.var_status.set("Leyendo coil…")
        self._log(f"[PLC READ] {nodo}.{sen} @ {host}:{port}")

        t = threading.Thread(
            target=self._plc_read_worker,
            args=(nodo, sen, host, port, self.ent_json.get().strip()),
            daemon=True
        )
        t.start()

    def _plc_read_worker(self, nodo, sen, host, port, ruta_json):
        try:
            val = leer_plc(nodo, sen, ruta_json=ruta_json, host=host, port=port)
            self._set_valor_ok(f"{nodo}.{sen} = {val}")
            self._log("[OK] Lectura de coil correcta.\n")
            self.var_status.set("Lectura de coil OK")
        except Exception as e:
            self._set_valor_err()
            self._log(f"[PLC READ ERROR] {type(e).__name__}: {e}\n")
            self.var_status.set("Error en lectura de coil")
        finally:
            self.btn_read_coil.config(state="normal")

    # --------------- Helpers ---------------
    def _set_valor_ok(self, text: str):
        self.var_valor.set(text)

    def _set_valor_err(self):
        self.var_valor.set("Error")

    def _log(self, msg: str):
        self.txt_log.insert("end", msg + "\n")
        self.txt_log.see("end")


if __name__ == "__main__":
    if not Path(RUTA_JSON_DEF).exists():
        print(f"[Aviso] No se encontró '{RUTA_JSON_DEF}' en el directorio actual.")
    app = ModbusGUI()
    app.mainloop()
