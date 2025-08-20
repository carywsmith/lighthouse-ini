# Lighthouse Modbus TCP Logger (INI / cron)

Same core functionality as the CLI version, but reads **all options from an INI file**. 
Great for running under **cron** or **systemd** without long command lines.

## Install
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# or minimal:
# pip install -r requirements-min.txt
```

## Configure
Edit `lighthouse.ini` (included). Seconds or minutes supported for Sample/Hold/Delay.

## Run
```bash
python lighthouse_modbus_logger_cron.py --config ./lighthouse.ini
```

## Cron example (every minute)
```
* * * * * /usr/bin/python3 /path/to/lighthouse_modbus_logger_cron.py --config /path/to/lighthouse.ini
```

## Notes
- Concentrations are `conc0p1ft3_ch*` (**particles/0.1 ft³**), with tag `count_uom="particles/0.1 ft3"`.
- Retries transient Modbus errors (6/11). Compatible with `pymodbus` versions using `unit=` or `slave=`.
