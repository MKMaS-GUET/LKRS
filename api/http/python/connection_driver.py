import requests
from requests.exceptions import RetryError

class Connection:
    DEFAULT_SERVER_IP = "127.0.0.1"
    DEFAULT_SERVER_PORT = 8998
    
    def __init__(self, db_name: str, ip: str = DEFAULT_SERVER_IP, port: int = DEFAULT_SERVER_PORT):
        self.base_url = f"http://{ip}:{port}/pisano"
        print(self.base_url)
        if not self.connect():
            print("connect failed!")

    def connect(self) -> bool:
        res = requests.get(self.base_url) 
        return res.status_code == 200
        
    def disconnect(self):
        pass

    def query(self, sparql: str):
        param = {"sparql": sparql}
        res = requests.post(self.base_url + "/query", data=param)
        if res.status_code == 200:
            data = res.json()
            return data
        else:
            print(res.status_code)
