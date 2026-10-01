import datetime
import requests
import threading
import yaml
import logging
import hashlib
import time
from cachetools import TTLCache, cached

# Handle imports for both standalone and Home Assistant contexts
try:
    from .classes.micro_inverter import Microinverter
    from .classes.solar_module import SolarModule
    from .classes.station import Station
    from .parsers import ProtobufParser
except ImportError:
    from classes.micro_inverter import Microinverter
    from classes.solar_module import SolarModule
    from classes.station import Station
    from parsers import ProtobufParser

_LOGGER = logging.getLogger(__name__)
_CACHE_LOCK = threading.RLock()


class HoymilesResponseError(ValueError):
    """The cloud returned a response without the expected data."""



class HoymilesClient:
    """
    Client for interacting with Hoymiles S-Cloud API.
    
    This class provides methods organized into different categories:
    - Authentication: login, token management
    - HTTP helpers: internal request methods
    - Data fetching: retrieve information from API
    - Control operations: send commands to devices
    - System mapping: build system hierarchy
    - Utilities: helper functions
    """
    
    def __init__(self, username, password, base_url):
        """Initialize the Hoymiles client with credentials and base URL."""
        _LOGGER.debug("Initializing HoymilesClient")
        
        self.username = username
        self.password = password
        self.base_url = base_url
        
        # API endpoint URIs
        self.uris = {
            "login": "iam/pub/0/auth/login",
            "user_info": "iam/api/1/user/me",
            "count_station_data": "pvm-data/api/0/station/data/count_station_real_data",
            "find": "pvm/api/0/station/find",
            "select_by_station": "pvm/api/0/dev/micro/select_by_station",
            "micro_find": "pvm/api/0/dev/micro/find",
            "module_details": "pvm-data/api/0/module/data/find_details",
            "select_all_arrays": "pvm/api/0/dev/array_v3/select_all",
            "down_module_day_data": "pvm-data/api/0/module/data/down_module_day_data",
            "down_station_day_data": "pvm-data/api/0/station/down_station_day_data",
            "select_device_of_tree": "pvm/api/0/station/select_device_of_tree",
        }
        
        self.token = None
        self._auth_lock = threading.RLock()
        self._authenticated_at = 0.0
        self._last_auth_retry = 0.0
        self.cache = TTLCache(maxsize=100, ttl=300)

    # ============================================================================
    # HTTP HELPER METHODS
    # ============================================================================


    def _post_request(self, uri, payload=None, headers=None, use_auth=True, binary=False,
                      response_type='json', retry_auth=True):
        """Helper method to make POST requests."""
        url = f"{self.base_url}{uri}"
        headers = dict(headers) if headers is not None else {"Content-Type": "application/json"}
        if use_auth:
            self._ensure_authenticated()
            sent_token = self.token
            headers["Authorization"] = sent_token

        _LOGGER.debug(f"POST Request URL: {url}")
        if uri != self.uris["login"]:
            _LOGGER.debug("POST Request Payload: %s", payload)
        
        response = requests.post(url, json=payload, headers=headers, timeout=20)
        response.raise_for_status()
        if response_type == 'protobuf' and binary:
            try:
                return ProtobufParser(response.content)
            except (ValueError, IndexError) as exc:
                raise HoymilesResponseError("Invalid Hoymiles module day data") from exc
        try:
            response_data = response.json()
        except ValueError as exc:
            raise HoymilesResponseError("Invalid JSON from Hoymiles") from exc

        # Hoymiles returned data="" on every endpoint exactly 24 hours after
        # login in the supplied log. Retry once after refreshing the token.
        if (use_auth and retry_auth and isinstance(response_data, dict)
                and response_data.get("data") == ""
                and self._refresh_expired_token(sent_token)):
            return self._post_request(uri, payload, headers, use_auth, binary,
                                      response_type, retry_auth=False)
        return response_data
        
    def _put_request(self, uri, payload=None, headers=None):
        """Helper method to make PUT requests."""
        return self._post_request(uri, payload=payload, headers=headers)

    def _ensure_authenticated(self):
        """Renew the shared session before its observed 24-hour expiry."""
        with self._auth_lock:
            if not self.token or time.monotonic() - self._authenticated_at >= 23 * 3600:
                self.login(force=True)

    def _refresh_expired_token(self, sent_token):
        """Let concurrent failed requests share one login, with a retry cooldown."""
        with self._auth_lock:
            if self.token != sent_token:
                return True
            if time.monotonic() - self._last_auth_retry < 300:
                return False
            self._last_auth_retry = time.monotonic()
            self.login(force=True)
            return True

    @staticmethod
    def _object_data(response, operation):
        """Reject malformed API data so cachetools does not cache a failed poll."""
        data = response.get("data") if isinstance(response, dict) else None
        if not isinstance(data, dict):
            raise HoymilesResponseError(f"Invalid {operation} response from Hoymiles")
        return data

    # ============================================================================
    # AUTHENTICATION METHODS
    # ============================================================================

    def get_password_hash(self):
      password = self.password.encode('utf-8')
      passwordHash = hashlib.md5(password)
      return passwordHash.hexdigest()

    def get_token(self,username, password):
      payload = {
          "user_name": username,
          "password": password,
      }
      return self._post_request(self.uris['login'], payload=payload, use_auth=False)
    

    def login(self, force=False):
        """Authenticate once per client, or renew a session that has expired."""
        with self._auth_lock:
            if self.token and not force:
                return True
            response_data = self.get_token(self.username, self.get_password_hash())
            data = response_data.get("data") if isinstance(response_data, dict) else None
            token = data.get("token") if isinstance(data, dict) else None
            if not token:
                raise HoymilesResponseError("Hoymiles authentication returned no token")
            self.token = token
            self._authenticated_at = time.monotonic()
            self._last_auth_retry = self._authenticated_at
            _LOGGER.info("Authenticated with Hoymiles S-Cloud")
            return True

    # ============================================================================
    # DATA FETCHING METHODS
    # ============================================================================

    @cached(cache=TTLCache(maxsize=100, ttl=300), lock=_CACHE_LOCK)
    def select_by_station(self, station_id):
        """Select microinverters by station ID."""
        payload = {
            "sid": station_id,
            "page": 1,
            "page_size": 1000,
            "show_warn": 0
        }
        response = self._post_request(self.uris['select_by_station'], payload=payload)

        return self._object_data(response, "microinverter list")
    
    @cached(cache=TTLCache(maxsize=100, ttl=300), lock=_CACHE_LOCK)
    def micro_find(self, micro_id, station_id):
        """Find a microinverter by its ID."""
        payload = {
            "id": micro_id,
            "sid": station_id,
        }
        response = self._post_request(self.uris['micro_find'], payload=payload)
        return self._object_data(response, "microinverter details")

    @cached(cache=TTLCache(maxsize=100, ttl=300), lock=_CACHE_LOCK)
    def module_details(self, station_id, micro_id, micro_sn, port, time):
        """Retrieve module details by its ID."""
        payload = {
            "sid": station_id,
            "mi_id": micro_id,
            "mi_sn": micro_sn,
            "port": port,
            "time": time,
            "warn_code": 1,
        }

        response = self._post_request(self.uris['module_details'], payload=payload)
        return self._object_data(response, "module details")

    @cached(cache=TTLCache(maxsize=100, ttl=300), lock=_CACHE_LOCK)
    def get_user_info(self):
        """Retrieve user information from Hoymiles S-Cloud. [UNUSED]"""
        return self._post_request(self.uris['user_info'])

    @cached(cache=TTLCache(maxsize=100, ttl=300), lock=_CACHE_LOCK)
    def select_by_page(self, type):
        uri_map = {
            "station":  "pvm/api/0/station/select_by_page",
            "dtu":      "pvm/api/0/dev/dtu/select_by_page",
            "micro":    "pvm/api/0/dev/micro/select_by_page",
            # Add more types here if needed
        }

        # Get the URI based on the type
        uri = uri_map.get(type)
        if not uri:
            raise ValueError(f"Invalid type for select_by_page: {type}")

        payload = {
            "page": 1,
            "page_size": 100,
        }
        response = self._post_request(uri, payload=payload)

        data = self._object_data(response, f"{type} list")
        stations = data.get("list")
        if not isinstance(stations, list):
            raise HoymilesResponseError(f"Invalid {type} list from Hoymiles")
        return stations

    @cached(cache=TTLCache(maxsize=100, ttl=300), lock=_CACHE_LOCK)
    def count_station_real_data(self,id):
        """Get the count of station real data."""
        _LOGGER.debug(f"Getting count of station real data for ID: {id}")
        payload = {
            "sid": id,
        }
        response = self._post_request(self.uris['count_station_data'], payload=payload)
        self._object_data(response, f"live data for station {id}")
        return response

    @cached(cache=TTLCache(maxsize=100, ttl=300), lock=_CACHE_LOCK)
    def findStation(self, sid):
        """Find a station by its ID."""
        payload = {
            "id": sid,
        }
        response = self._post_request(self.uris['find'], payload=payload)
        return self._object_data(response, f"station {sid}")

    def down_module_day_data(self, sid, date):
        """Download module day data for a specific date."""
        payload = {
            "sid": sid,
            "date": date,
        }
        response = self._post_request(self.uris['down_module_day_data'], payload=payload, response_type='protobuf', binary=True)
        return response

    @cached(cache=TTLCache(maxsize=100, ttl=300), lock=_CACHE_LOCK)
    def select_device_of_tree(self, station_id):
        """Get device tree for a station including DTU and microinverters."""
        payload = {
            "id": station_id,
        }
        response = self._post_request(self.uris['select_device_of_tree'], payload=payload)
        data = response.get('data') if isinstance(response, dict) else None
        if not isinstance(data, list):
            raise HoymilesResponseError(f"Invalid device tree for station {station_id}")
        return data

    def parse_dtu_info(self, tree_data):
        """Parse DTU information from select_device_of_tree response.
        
        Returns a list of DTU info dictionaries with:
        - id: DTU ID
        - sn: Serial number
        - model_no: Model name
        - connect: Connection status
        - soft_ver: Software version
        - hard_ver: Hardware version
        - microinverters: List of child microinverter IDs
        """
        dtu_list = []
        
        for device in tree_data:
            if device.get('type') == 1:  # Type 1 appears to be DTU
                dtu_info = {
                    'id': device.get('id'),
                    'sn': device.get('sn'),
                    'model_no': device.get('model_no'),
                    'connect': device.get('warn_data', {}).get('connect', False),
                    'soft_ver': device.get('soft_ver'),
                    'hard_ver': device.get('hard_ver'),
                    'microinverters': [child.get('id') for child in device.get('children', [])]
                }
                dtu_list.append(dtu_info)
        
        return dtu_list

    # ============================================================================
    # CONTROL OPERATIONS
    # ============================================================================

    
    def turn_off_microinverter(self, dev_sn, dev_type, dtu_sn):
        """Turn off the microinverter. [UNUSED]"""
        self.put_command(7, dev_sn, 3, dtu_sn)

    def set_power_limit(self, sid, power_limit):
        """Set the power limit for a microinverter."""
        power_limit = min(100, int(power_limit))
        power_limit = max(5, int(power_limit))
        payload = {
            "action": 8,
            "data": {
                "sid": sid,
                "power_limit": power_limit,
                "enable": 1
            }
        }

        _LOGGER.debug(f"Setting power limit for SID {sid} to {power_limit}%")
        
        uri = 'pvm-ctl/api/0/dev/command/put'
        return self._put_request(uri, payload=payload)

    # ============================================================================
    # SYSTEM MAPPING AND DATA PROCESSING
    # ============================================================================
    

    def map_system(self):
        """Build a hierarchical system map of stations, microinverters, and modules."""
        stations = self.select_by_page("station")
        system = []
        if not stations:
            _LOGGER.warning("No stations found.")
            return system
        
        for station_data in stations:
            station = Station(station_data.get("id"), station_data.get("name"))
            
            # Fetch DTU information for the station
            try:
                tree_data = self.select_device_of_tree(station.station_id)
                dtus = self.parse_dtu_info(tree_data)
                station.set_dtus(dtus)
                _LOGGER.debug(f"Found {len(dtus)} DTU(s) for station {station.station_id}")
            except Exception as e:
                _LOGGER.warning(f"Could not fetch DTU info for station {station.station_id}: {e}")
            
            # Fetch microinverters for the station
            microinverters = self.select_by_station(station.station_id)
            if not microinverters:
                _LOGGER.warning(f"No microinverters found for station ID {station.station_id}.")
                continue
                
            for micro_data in microinverters.get("list", []):
                micro_id = micro_data.get("id")
                sn = micro_data.get("sn")
                microinverter = Microinverter(micro_id, sn)
                station.add_microinverter(microinverter)
                
                # Fetch module details for each port
                micro_details = self.micro_find(micro_id, station.station_id)
                for port_info in micro_details.get("layout_list", []):
                    module_id = f"{sn}-{port_info.get('port')}"
                    port = port_info.get("port")
                    x = port_info.get("x")
                    y = port_info.get("y")
                    solar_module = SolarModule(module_id, port, x, y)
                    microinverter.add_module(solar_module)
                    
            system.append(station)
        return system
    
    
    def fill_system_data(self, system, date=None):
        """Fill system hierarchy with actual performance data for a given date."""
        if date is None:
            date = datetime.datetime.now().strftime("%Y-%m-%d")
        _LOGGER.debug(f"Filling system data for date: {date}")
        for station in system:
            sid = station.station_id
            data = self.down_module_day_data(sid, date)
            station.set_data(data)
