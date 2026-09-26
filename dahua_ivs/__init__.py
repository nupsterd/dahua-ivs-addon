"""Add-on de Home Assistant: eventos IVS (tripwire) de una cámara Dahua.

Es un MENSAJERO: consume el stream ``eventManager.cgi?action=attach`` de la
cámara, se queda solo con el ``action=Start`` de cada cruce, lo recorta a un
registro plano, lo audita en disco y, si hay backend configurado, lo encola en
una cola SQLite persistente que lo reenvía por POST. No correlaciona nada: eso
lo hace el backend (B5.3).
"""

ADDON_VERSION = "0.1.1-alpha"
DEVICE_KIND = "camera_tripwire"
