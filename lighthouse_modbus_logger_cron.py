#!/usr/bin/env python3
# Lighthouse ApexRp / Remote P Modbus TCP Logger (INI-driven)
# Reads all options from an INI file for cron/systemd runs.

import argparse
import configparser
import os
import sys

# ----------------- BEGIN: Embedded LighthouseLogger -----------------
import logging
import struct
import time
from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple, Any
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    HAVE_INFLUX = True
except Exception:
    HAVE_INFLUX = False
    InfluxDBClient = None
    Point = None
    WritePrecision = None

def h_addr(reg: int) -> int:
    if reg < 40001: raise ValueError(f"Holding register must be >= 40001, got {reg}")
    return reg - 40001
def i_addr(reg: int) -> int:
    if reg < 30001: raise ValueError(f"Input register must be >= 30001, got {reg}")
    return reg - 30001
def regs_to_u32(high: int, low: int) -> int:
    return ((high & 0xFFFF) << 16) | (low & 0xFFFF)
def regs_to_i32(high: int, low: int) -> int:
    val = regs_to_u32(high, low)
    return struct.unpack('>i', struct.pack('>I', val))[0]
def regs_to_float_ieee(high: int, low: int, word_order_big_endian: bool = True) -> float:
    if word_order_big_endian: raw = struct.pack('>HH', high & 0xFFFF, low & 0xFFFF)
    else:                      raw = struct.pack('>HH', low & 0xFFFF, high & 0xFFFF)
    return struct.unpack('>f', raw)[0]
def decode_ascii_words(words: List[int]) -> str:
    bs = bytearray()
    for w in words:
        bs.append((w >> 8) & 0xFF); bs.append(w & 0xFF)
    if 0 in bs: bs = bs[:bs.index(0)]
    try: return bs.decode('ascii', errors='ignore').strip()
    except Exception: return ''

@dataclass
class DeviceProfile:
    name: str
    type: str
    FLOW_REG: int = 40023
    DEVICE_STATUS: int = 40003
    EXT_STATUS: Optional[int] = None
    ALARM_PARTICLE_HI: Optional[int] = None
    ALARM_PARTICLE_LO: Optional[int] = None
    ALARM_ANALOG_HI: Optional[int] = None
    ALARM_ANALOG_LO: Optional[int] = None
    PRODUCT_NAME_START: int = 40007
    PRODUCT_NAME_LEN: int = 8
    MODEL_NAME_START: int = 40015
    MODEL_NAME_LEN: int = 8

APEX_PROFILE = DeviceProfile(
    name="ApexRp", type="apex", EXT_STATUS=40056,
    ALARM_PARTICLE_HI=40064, ALARM_PARTICLE_LO=40065,
    ALARM_ANALOG_HI=40066, ALARM_ANALOG_LO=40067,
)
REMOTE_PROFILE = DeviceProfile(name="REMOTE P", type="remote", EXT_STATUS=None)

class LighthouseLogger:
    def __init__(self, host, port, unit_id,
                 influx_url, influx_org, influx_bucket, influx_token,
                 interval=5.0, max_channels=6, verbose=False, log_file=None,
                 enable_influx=True):
        self.host = host; self.port = port; self.unit_id = unit_id
        self.interval = interval; self.max_channels = max_channels
        self.client = ModbusTcpClient(host=host, port=port, timeout=3)
        self.profile: Optional[DeviceProfile] = None
        self.product_name = ''; self.model_name = ''; self.register_map_version = None
        self._influx_bucket = influx_bucket; self._influx_org = influx_org
        self.enable_influx = bool(enable_influx) and HAVE_INFLUX and bool(influx_token)
        if self.enable_influx:
            self.influx = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
            self.write_api = self.influx.write_api()
        else:
            self.influx = None; self.write_api = None

        self.logger = logging.getLogger("lighthouse-cron")
        self.logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt)
        sh.setLevel(logging.DEBUG if verbose else logging.INFO)
        self.logger.addHandler(sh)
        if log_file:
            fh = logging.FileHandler(log_file); fh.setFormatter(fmt)
            fh.setLevel(logging.DEBUG if verbose else logging.INFO)
            self.logger.addHandler(fh)
        if not self.enable_influx:
            why = "disabled" if enable_influx is False else ("package not installed" if not HAVE_INFLUX else "missing token")
            self.logger.info("InfluxDB writes are OFF (%s).", why)
        self.temp_cfg = None; self.rh_cfg = None

    def _read_h(self, start_reg: int, count: int = 1):
        addr = h_addr(start_reg); fn = self.client.read_holding_registers
        for attempt in range(3):
            try: rr = fn(address=addr, count=count, unit=self.unit_id)
            except TypeError:
                try: rr = fn(address=addr, count=count, slave=self.unit_id)
                except TypeError: rr = fn(address=addr, count=count)
            if rr.isError():
                code = getattr(rr, 'exception_code', None)
                if code in (6,11) and attempt < 2: time.sleep(0.25*(2**attempt)); continue
                self.logger.error(f"Read holding {start_reg} x{count} failed: {rr}"); return None
            return list(rr.registers)
        return None
    def _write_h(self, reg: int, value: int) -> bool:
        addr = h_addr(reg); fn = self.client.write_register
        try: wr = fn(address=addr, value=value & 0xFFFF, unit=self.unit_id)
        except TypeError:
            try: wr = fn(address=addr, value=value & 0xFFFF, slave=self.unit_id)
            except TypeError: wr = fn(address=addr, value=value & 0xFFFF)
        if wr.isError(): self.logger.error(f"Write holding {reg}={value} failed: {wr}"); return False
        return True
    def _read_i(self, start_reg: int, count: int = 1):
        addr = i_addr(start_reg); fn = self.client.read_input_registers
        for attempt in range(3):
            try: rr = fn(address=addr, count=count, unit=self.unit_id)
            except TypeError:
                try: rr = fn(address=addr, count=count, slave=self.unit_id)
                except TypeError: rr = fn(address=addr, count=count)
            if rr.isError():
                code = getattr(rr, 'exception_code', None)
                if code in (6,11) and attempt < 2: time.sleep(0.25*(2**attempt)); continue
                self.logger.error(f"Read input {start_reg} x{count} failed: {rr}"); return None
            return list(rr.registers)
        return None
    def _read_scalar_input(self, reg: int, dtype: str) -> Optional[float]:
        dt = (dtype or "").lower()
        if dt in ("u16","i16"):
            r = self._read_i(reg,1); 
            if not r: return None
            v = r[0] & 0xFFFF
            if dt == "i16" and v >= 0x8000: v -= 0x10000
            return float(v)
        elif dt in ("u32","i32","float","float_be","float_le"):
            r = self._read_i(reg,2); 
            if not r or len(r)<2: return None
            hi, lo = r[0], r[1]
            if dt == "u32": return float(regs_to_u32(hi,lo))
            if dt == "i32": return float(regs_to_i32(hi,lo))
            be = True if dt in ("float","float_be") else False
            return float(regs_to_float_ieee(hi,lo,word_order_big_endian=be))
        else:
            self.logger.error(f"Unsupported dtype '{dtype}'"); return None
    def detect_device(self) -> 'DeviceProfile':
        pn = self._read_h(APEX_PROFILE.PRODUCT_NAME_START, APEX_PROFILE.PRODUCT_NAME_LEN) or []
        mn = self._read_h(APEX_PROFILE.MODEL_NAME_START, APEX_PROFILE.MODEL_NAME_LEN) or []
        product = decode_ascii_words(pn) if pn else ''; model = decode_ascii_words(mn) if mn else ''
        self.product_name = product; self.model_name = model
        text = f"{product} {model}".upper()
        if "APEX" in text: self.profile = APEX_PROFILE
        elif "REMOTE" in text or "3014P" in text or "5014P" in text or "2014P" in text: self.profile = REMOTE_PROFILE
        else: self.profile = REMOTE_PROFILE
        ver = self._read_h(40001, 1); 
        if ver: self.register_map_version = ver[0]
        self.logger.info(f"Detected device: type={self.profile.type} product='{product}' model='{model}' regmap={self.register_map_version}")
        return self.profile
    @staticmethod
    def _decode_device_status_bits(val: int, device_type: str) -> Dict[str,int]:
        bits = {"running":(val>>0)&1,"sampling":(val>>1)&1,"new_data":(val>>2)&1,"device_error":(val>>3)&1}
        if device_type=='apex':
            bits.update({"data_validation":(val>>9)&1,"location_validation":(val>>10)&1,"laser_status":(val>>11)&1,
                         "flow_status":(val>>12)&1,"service_status":(val>>13)&1,"threshold_high":(val>>14)&1,"threshold_low":(val>>15)&1})
        return bits
    def set_times(self, sample=None, hold=None, delay=None, save=True):
        updated=False
        if sample is not None: self._write_h(40034, int(sample)&0xFFFF); updated=True; self.logger.info("Set Sample=%s s", sample)
        if hold   is not None: self._write_h(40032, int(hold)&0xFFFF);   updated=True; self.logger.info("Set Hold=%s s", hold)
        if delay  is not None: self._write_h(40030, int(delay)&0xFFFF);  updated=True; self.logger.info("Set Delay=%s s", delay)
        if updated and save: self._write_h(40002, 4); self.logger.info("Saved parameters to device.")
    def start(self, wait=True, timeout=30.0)->bool:
        self._write_h(40002, 11); self.logger.info("Sent START command (40002=11).")
        if not wait: return True
        t0=time.time()
        while time.time()-t0<timeout:
            ds=self._read_h(40003,1)
            if ds:
                if (ds[0]&1) or (ds[0]>>1)&1: return True
            time.sleep(0.5)
        return False
    def stop(self): self._write_h(40002,12); self.logger.info("Sent STOP command (40002=12).")
    def poll_once(self)->Optional[Dict]:
        if not self.profile: self.detect_device()
        flow_regs = self._read_h(self.profile.FLOW_REG,1); flow_cfm = (flow_regs[0]/100.0) if flow_regs else None
        ds_regs = self._read_h(self.profile.DEVICE_STATUS,1); dsv = ds_regs[0] if ds_regs else 0
        ds_bits = self._decode_device_status_bits(dsv, self.profile.type)
        ext_status_val=None
        if self.profile.EXT_STATUS:
            er=self._read_h(self.profile.EXT_STATUS,1); 
            if er: ext_status_val=er[0]
        alarms={}
        if self.profile.type=='apex':
            for key,reg in (("particle_alarm_high_flags",self.profile.ALARM_PARTICLE_HI),
                            ("particle_alarm_low_flags",self.profile.ALARM_PARTICLE_LO),
                            ("analog_alarm_high_flags",self.profile.ALARM_ANALOG_HI),
                            ("analog_alarm_low_flags",self.profile.ALARM_ANALOG_LO)):
                if reg:
                    r=self._read_h(reg,1); 
                    if r: alarms[key]=r[0]
        self._write_h(40025,0xFFFF)
        data_regs=self._read_i(30001,20)
        timestamp=None; sample_time_s=None; counts={}
        if data_regs and len(data_regs)>=20:
            timestamp = regs_to_u32(data_regs[0],data_regs[1])
            sample_time_s = regs_to_u32(data_regs[2],data_regs[3])
            start_idx=8
            for ch in range(1,7):
                idx=start_idx + (ch-1)*2
                if idx+1 < len(data_regs):
                    counts[f"ch{ch}"] = regs_to_u32(data_regs[idx], data_regs[idx+1])
            ss_val = regs_to_u32(data_regs[6],data_regs[7])
            if self.profile.type=='remote':
                alarms["sample_status"]=ss_val
                alarms["threshold_high"]=(ss_val>>4)&1
                alarms["threshold_low"]=(ss_val>>5)&1
        temp_c=rh_percent=None
        if self.temp_cfg and self.temp_cfg.get("reg"):
            v=self._read_scalar_input(self.temp_cfg["reg"], self.temp_cfg.get("dtype","float"))
            if v is not None: temp_c = (v*float(self.temp_cfg.get("scale",1.0)))+float(self.temp_cfg.get("offset",0.0))
        if self.rh_cfg and self.rh_cfg.get("reg"):
            v=self._read_scalar_input(self.rh_cfg["reg"], self.rh_cfg.get("dtype","float"))
            if v is not None: rh_percent = (v*float(self.rh_cfg.get("scale",1.0)))+float(self.rh_cfg.get("offset",0.0))
        conc_0p1ft3={}; sample_volume_cuft=None
        try:
            if flow_cfm is not None and sample_time_s and sample_time_s>0:
                sample_volume_cuft=(flow_cfm*float(sample_time_s))/60.0
                if sample_volume_cuft>0:
                    for ch,cnt in counts.items(): conc_0p1ft3[ch]=float(cnt)*0.1/sample_volume_cuft
        except Exception: pass
        return {"device_type":self.profile.type,"product_name":self.product_name,"model_name":self.model_name,
                "regmap_version":self.register_map_version,"flow_cfm":flow_cfm,"device_status_val":dsv,
                "device_status":ds_bits,"ext_status_val":ext_status_val,"alarms":alarms,"timestamp":timestamp,
                "sample_time_s":sample_time_s,"counts":counts,"sample_volume_cuft":sample_volume_cuft,
                "conc_0p1ft3":conc_0p1ft3,"temp_c":temp_c,"rh_percent":rh_percent}
    def write_influx(self, record: Dict):
        if not self.enable_influx or not self.write_api or not HAVE_INFLUX: return
        tags={"device_type":record.get("device_type",""),"product_name":record.get("product_name",""),
              "model_name":record.get("model_name",""),"host":self.host,"unit_id":str(self.unit_id),
              "count_uom":"particles/0.1 ft3"}
        from influxdb_client import Point, WritePrecision
        point = Point("lighthouse").tag("device_type",tags["device_type"]).tag("product_name",tags["product_name"])\
            .tag("model_name",tags["model_name"]).tag("host",tags["host"]).tag("unit_id",tags["unit_id"])\
            .tag("count_uom",tags["count_uom"])
        if record.get("flow_cfm") is not None: point=point.field("flow_cfm", float(record["flow_cfm"]))
        point=point.field("device_status_val", int(record.get("device_status_val") or 0))
        for k,v in (record.get("device_status") or {}).items(): point=point.field(f"status_{k}", int(v))
        if record.get("ext_status_val") is not None: point=point.field("ext_status_val", int(record["ext_status_val"]))
        for k,v in (record.get("alarms") or {}).items(): point=point.field(f"alarm_{k}", int(v))
        for k,v in (record.get("counts") or {}).items(): point=point.field(f"count_{k}", int(v))
        for k,v in (record.get("conc_0p1ft3") or {}).items(): point=point.field(f"conc0p1ft3_{k}", float(v))
        if record.get("sample_volume_cuft") is not None: point=point.field("sample_volume_cuft", float(record["sample_volume_cuft"]))
        if record.get("temp_c") is not None: point=point.field("temp_c", float(record["temp_c"]))
        if record.get("rh_percent") is not None: point=point.field("rh_percent", float(record["rh_percent"]))
        ts=record.get("timestamp")
        if ts is not None and ts>0: point=point.time(int(ts), WritePrecision.S)
        else: point=point.time(time.time_ns(), WritePrecision.NS)
        self.write_api.write(bucket=self._influx_bucket, org=self._influx_org, record=point)
    def run(self):
        if not self.client.connect():
            self.logger.error(f"Unable to connect to {self.host}:{self.port}"); sys.exit(2)
        try:
            self.detect_device()
            if getattr(self,'auto_start',False):
                self.set_times(sample=getattr(self,'start_sample',1),
                               hold=getattr(self,'start_hold',14),
                               delay=getattr(self,'start_delay',0),
                               save=getattr(self,'save_params',True))
                ok=self.start(wait=True,timeout=30.0)
                self.logger.info("Auto-start %s (running=%s)", "OK" if ok else "TIMED OUT", ok)
                time.sleep(1.0)
            while True:
                try:
                    rec=self.poll_once()
                    if rec: self.write_influx(rec); self.logger.debug(f"Wrote record: {rec}")
                except ModbusException as me: self.logger.error(f"Modbus exception: {me}")
                except Exception as e: self.logger.exception(f"Error during poll: {e}")
                time.sleep(self.interval)
        finally:
            self.client.close()
            try:
                if self.influx: self.influx.__del__()
            except Exception: pass
# ----------------- END: Embedded LighthouseLogger -----------------

def parse_bool(s: str, default=False):
    if s is None: return default
    return str(s).strip().lower() in ("1","true","yes","on")

def parse_config(path: str) -> dict:
    cfg = configparser.ConfigParser()
    if not cfg.read(path):
        print(f"[ERROR] Could not read config file: {path}", file=sys.stderr)
        sys.exit(2)

    out = {}

    # [device]
    host = cfg.get("device", "host", fallback=None)
    if not host:
        print("[ERROR] [device].host is required", file=sys.stderr); sys.exit(2)
    out["host"] = host
    out["port"] = cfg.getint("device", "port", fallback=502)
    out["unit_id"] = cfg.getint("device", "unit_id", fallback=1)
    out["interval"] = cfg.getfloat("device", "interval", fallback=5.0)
    out["max_channels"] = cfg.getint("device", "max_channels", fallback=6)

    # [logging]
    out["verbose"] = parse_bool(cfg.get("logging","verbose",fallback="false"))
    out["log_file"] = cfg.get("logging", "log_file", fallback=None)

    # [autostart]
    out["auto_start"] = parse_bool(cfg.get("autostart","enabled",fallback="false"))
    # seconds or minutes (minutes win if provided)
    sample = cfg.get("autostart","sample",fallback=None)
    hold   = cfg.get("autostart","hold",fallback=None)
    delay  = cfg.get("autostart","delay",fallback=None)
    s_min  = cfg.get("autostart","sample_min",fallback=None)
    h_min  = cfg.get("autostart","hold_min",fallback=None)
    d_min  = cfg.get("autostart","delay_min",fallback=None)
    out["start_sample"] = int(float(sample)) if (sample is not None and s_min is None) else (int(float(s_min)*60) if s_min is not None else 1)
    out["start_hold"]   = int(float(hold))   if (hold   is not None and h_min is None) else (int(float(h_min)*60) if h_min is not None else 14)
    out["start_delay"]  = int(float(delay))  if (delay  is not None and d_min is None) else (int(float(d_min)*60) if d_min is not None else 0)
    out["save_params"]  = parse_bool(cfg.get("autostart","save_params",fallback="true"))

    # [env]
    temp_reg = cfg.get("env","temp_reg",fallback=None)
    if temp_reg: 
        out["temp_cfg"]={"reg":int(temp_reg),
                         "dtype":cfg.get("env","temp_type",fallback="float"),
                         "scale":cfg.getfloat("env","temp_scale",fallback=1.0),
                         "offset":cfg.getfloat("env","temp_offset",fallback=0.0)}
    else:
        out["temp_cfg"]=None
    rh_reg = cfg.get("env","rh_reg",fallback=None)
    if rh_reg:
        out["rh_cfg"]={"reg":int(rh_reg),
                       "dtype":cfg.get("env","rh_type",fallback="float"),
                       "scale":cfg.getfloat("env","rh_scale",fallback=1.0),
                       "offset":cfg.getfloat("env","rh_offset",fallback=0.0)}
    else:
        out["rh_cfg"]=None

    # [influx]
    enabled = parse_bool(cfg.get("influx","enabled",fallback="false"))
    out["enable_influx"] = enabled
    out["influx_url"]    = cfg.get("influx","url",fallback=os.environ.get("INFLUX_URL","http://localhost:8086"))
    out["influx_org"]    = cfg.get("influx","org",fallback=os.environ.get("INFLUX_ORG","default-org"))
    out["influx_bucket"] = cfg.get("influx","bucket",fallback=os.environ.get("INFLUX_BUCKET","lighthouse"))
    out["influx_token"]  = cfg.get("influx","token",fallback=os.environ.get("INFLUX_TOKEN",""))

    return out

def parse_args():
    ap = argparse.ArgumentParser(description="Lighthouse logger (INI-driven)")
    ap.add_argument("--config", default="./lighthouse.ini", help="Path to INI config (default ./lighthouse.ini)")
    return ap.parse_args()

def main():
    args = parse_args()
    cfg = parse_config(args.config)

    logger = LighthouseLogger(
        host=cfg["host"], port=cfg["port"], unit_id=cfg["unit_id"],
        influx_url=cfg["influx_url"], influx_org=cfg["influx_org"],
        influx_bucket=cfg["influx_bucket"], influx_token=cfg["influx_token"],
        interval=cfg["interval"], max_channels=cfg["max_channels"],
        verbose=cfg["verbose"], log_file=cfg["log_file"],
        enable_influx=cfg["enable_influx"]
    )

    # Apply autostart/env options
    logger.auto_start = bool(cfg["auto_start"])
    logger.start_sample = cfg["start_sample"]; logger.start_hold = cfg["start_hold"]; logger.start_delay = cfg["start_delay"]
    logger.save_params = cfg["save_params"]
    logger.temp_cfg = cfg["temp_cfg"]; logger.rh_cfg = cfg["rh_cfg"]

    logger.run()

if __name__ == "__main__":
    main()
