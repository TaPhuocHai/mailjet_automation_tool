import platform
import os
import urllib.request
import subprocess
import time

"""
Reference to MacOs/Unix 
https://www.unix.com/man-page/mojave/8/networksetup/

Reference for Windows
https://serverfault.com/questions/212227/how-can-i-disconnect-from-openvpn-in-windows-via-command-line
"""

def check_connectivity(reference):
    try:
        urllib.request.urlopen(reference, timeout=1)
        return True
    except urllib.request.URLError:
        return False


def connectVPN():
    print("Attempting to connect to Ematic VPN automatically")
    if platform.system() == "Windows":
        cmd = r'"c:\Program Files\OpenVPN\bin\openvpn-gui.exe" --command connect ematic'
        # return os.system(cmd)
        return subprocess.check_call(cmd)
    elif platform.system() == "Darwin":
        return os.system("networksetup -connectpppoeservice ematic") 
    elif platform.system() == "Linux":
        return os.system("nmcli connection up ematic")


def disconnectVPN():
    print("Attempting to disconnect to Ematic VPN automatically")
    if platform.system() == "Windows":
        cmd = r'"c:\Program Files\OpenVPN\bin\openvpn-gui.exe" --command disconnect ematic'
        os.system(cmd)
    elif platform.system() == "Darwin": # https://www.unix.com/man-page/mojave/8/networksetup/
        os.system("networksetup -disconnectpppoeservice ematic")
    elif platform.system() == "Linux":
        os.system("nmcli connection down ematic")


