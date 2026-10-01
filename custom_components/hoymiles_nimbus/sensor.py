# custom_components/hoymiles_cloud/sensor.py

import asyncio
import logging
import time

import requests
from homeassistant.components.sensor import SensorEntity, SensorStateClass
from homeassistant.const import UnitOfPower, UnitOfEnergy, UnitOfElectricPotential, UnitOfElectricCurrent
from homeassistant.helpers import device_registry as dr

from .hoymiles_client import HoymilesResponseError
from .device_registry import create_station_device_info, create_module_device_info

DOMAIN = "hoymiles_nimbus"

_LOGGER = logging.getLogger(__name__)


class HoymilesSystemCoordinator:
    """Share one refresh across all module sensors."""

    def __init__(self, hass, client, initial_system):
        self._hass = hass
        self._client = client
        self._system = initial_system
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()
        self.available = True

    async def get_system(self):
        async with self._lock:
            if time.monotonic() - self._last_update >= 30:
                # Wait before retrying a failed poll; keep the previous valid map.
                self._last_update = time.monotonic()
                try:
                    system = await self._hass.async_add_executor_job(self._client.map_system)
                    await self._hass.async_add_executor_job(self._client.fill_system_data, system)
                except (HoymilesResponseError, requests.exceptions.RequestException) as exc:
                    if self.available:
                        _LOGGER.warning("Hoymiles module data unavailable: %s", exc)
                    self.available = False
                else:
                    if not self.available:
                        _LOGGER.info("Hoymiles module data available again")
                    self._system = system
                    self.available = True
            return self._system if self.available else None

    def find_module(self, station_id, module_id):
        for station in self._system:
            if station.station_id == station_id:
                for microinverter in station.microinverters:
                    for module in microinverter.modules:
                        if module.id == module_id:
                            return module
        return None


class HoymilesStationCoordinator:
    """Poll a station once for its four sensors and share failures."""

    def __init__(self, hass, client, station_id):
        self._hass = hass
        self._client = client
        self._station_id = station_id
        self._lock = asyncio.Lock()
        self._last_update = 0.0
        self._data = None
        self._available = True

    async def get_data(self):
        async with self._lock:
            if time.monotonic() - self._last_update >= 30:
                self._last_update = time.monotonic()
                try:
                    response = await self._hass.async_add_executor_job(
                        self._client.count_station_real_data, self._station_id
                    )
                except (HoymilesResponseError, requests.exceptions.RequestException) as exc:
                    if self._available:
                        _LOGGER.warning("Live data unavailable for station %s: %s", self._station_id, exc)
                    self._data = None
                    self._available = False
                else:
                    if not self._available:
                        _LOGGER.info("Live data available again for station %s", self._station_id)
                    self._data = response["data"]
                    self._available = True
            return self._data


async def async_setup_entry(hass, config_entry, async_add_entities):
    client = hass.data[DOMAIN][config_entry.entry_id]

    # Ensure the client is authenticated
    await hass.async_add_executor_job(client.login)

    _LOGGER.warning("Fetching device data from Hoymiles S-Cloud...")
    stations = await hass.async_add_executor_job(client.select_by_page, "station")

    # Build system map with individual modules
    system = await hass.async_add_executor_job(client.map_system)
    await hass.async_add_executor_job(client.fill_system_data, system)

    _LOGGER.warning("Found %d station(s) in Hoymiles account", len(stations))

    entities = []
    station_device_ids = {}
    device_registry = dr.async_get(hass)
    
    # Create a shared system coordinator for all module sensors
    system_coordinator = HoymilesSystemCoordinator(hass, client, system)
    
    for station in stations:
        station_name = station.get('name', 'Unknown')
        sid = station.get("id")
        device_info = create_station_device_info(sid, station_name)
        name = device_info["name"]

        # Register the station before its modules need the parent device ID.
        # Reuse the existing identifier so established HA devices remain intact.
        station_device = device_registry.async_get_or_create(
            config_entry_id=config_entry.entry_id, **device_info
        )
        station_device_ids[sid] = station_device.id

        station_coordinator = HoymilesStationCoordinator(hass, client, sid)
        entities.append(HoymilesStationPowerSensor(station_coordinator, name, sid, device_info))
        entities.append(HoymilesStationEnergySensor(station_coordinator, name, sid, device_info))
        entities.append(HoymilesStationTotalEnergySensor(station_coordinator, name, sid, device_info))
        entities.append(HoymilesStationRatioSensor(station_coordinator, name, sid, device_info))

    # Add individual solar module sensors
    for station in system:
        station_name = station.name
        sid = station.station_id
        station_device_id = station_device_ids.get(sid)
        if station_device_id is None:
            _LOGGER.warning("No registered station device for %s; skipping its modules", sid)
            continue

        for microinverter in station.microinverters:
            for module in microinverter.modules:
                module_name = f"{station_name} Panel {module.id}"
                
                # Create device info for the solar module
                module_device_info = create_module_device_info(module.id, station_device_id)
                
                # Add power, voltage, and current sensors for each module
                entities.append(HoymilesSolarModulePowerSensor(system_coordinator, module_name, station.station_id, module, module_device_info))
                entities.append(HoymilesSolarModuleVoltageSensor(system_coordinator, module_name, station.station_id, module, module_device_info))
                entities.append(HoymilesSolarModuleCurrentSensor(system_coordinator, module_name, station.station_id, module, module_device_info))

    _LOGGER.warning("Created %d sensors for Hoymiles devices", len(entities))
    async_add_entities(entities)

class HoymilesStationSensor(SensorEntity):
    """Common state handling for values from a station's live data."""

    def __init__(self, coordinator, name, sid, device_info):
        self._coordinator = coordinator
        self._sid = sid
        self._attr_device_info = device_info
        self._state = None

    @property
    def native_value(self):
        return self._state

    async def _read_data(self):
        data = await self._coordinator.get_data()
        self._attr_available = data is not None
        if data is None:
            self._state = None
        return data

    def _set_metric(self, data, key, scale=1):
        value = data.get(key)
        if value is None:
            self._attr_available = False
            self._state = None
            return
        try:
            self._state = float(value) / scale
        except (TypeError, ValueError):
            self._attr_available = False
            self._state = None
            _LOGGER.warning("Invalid %s value for station %s", key, self._sid)


class HoymilesStationPowerSensor(HoymilesStationSensor):
    def __init__(self, coordinator, name, sid, device_info):
        super().__init__(coordinator, name, sid, device_info)
        self._attr_name = f"{name} Current Power"
        self._attr_native_unit_of_measurement = UnitOfPower.WATT
        self._attr_unique_id = f"hoymiles_nimbus_{sid}_power"
        self._attr_device_class = "power"
        self._attr_state_class = SensorStateClass.MEASUREMENT

    async def async_update(self):
        if (data := await self._read_data()) is not None:
            self._set_metric(data, "real_power")


class HoymilesStationEnergySensor(HoymilesStationSensor):
    def __init__(self, coordinator, name, sid, device_info):
        super().__init__(coordinator, name, sid, device_info)
        self._attr_name = f"{name} Daily Energy"
        self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        self._attr_unique_id = f"hoymiles_nimbus_{sid}_energy"
        self._attr_device_class = "energy"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_icon = "mdi:solar-power"

    async def async_update(self):
        if (data := await self._read_data()) is not None:
            self._set_metric(data, "today_eq", 1000)


class HoymilesStationTotalEnergySensor(HoymilesStationSensor):
    def __init__(self, coordinator, name, sid, device_info):
        super().__init__(coordinator, name, sid, device_info)
        self._attr_name = f"{name} Total Energy"
        self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        self._attr_unique_id = f"hoymiles_nimbus_{sid}_total_energy"
        self._attr_device_class = "energy"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_icon = "mdi:solar-power"

    async def async_update(self):
        if (data := await self._read_data()) is not None:
            self._set_metric(data, "total_eq", 1000)


class HoymilesStationRatioSensor(HoymilesStationSensor):
    def __init__(self, coordinator, name, sid, device_info):
        super().__init__(coordinator, name, sid, device_info)
        self._attr_name = f"{name} Performance Ratio"
        self._attr_native_unit_of_measurement = "%"
        self._attr_unique_id = f"hoymiles_nimbus_{sid}_performance_ratio"
        self._attr_device_class = "performance_ratio"
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:percent"

    async def async_update(self):
        data = await self._read_data()
        if data is None:
            return
        capacity = data.get("capacitor")
        power = data.get("real_power")
        if capacity is None or power is None:
            self._state = None
            self._attr_available = False
            return
        try:
            capacity = float(capacity)
            power = float(power)
            self._state = round(power / (capacity * 1000) * 100, 2) if capacity > 0 else 0
        except (TypeError, ValueError):
            self._state = None
            self._attr_available = False
            _LOGGER.warning("Invalid performance data for station %s", self._sid)


class HoymilesSolarModulePowerSensor(SensorEntity):
    def __init__(self, coordinator, name, station_id, module, device_info):
        self._coordinator = coordinator
        self._station_id = station_id
        self._module_id = module.id
        self._attr_name = f"{name} Power"
        self._attr_native_unit_of_measurement = UnitOfPower.WATT
        self._attr_unique_id = f"hoymiles_nimbus_module_{module.id}_power"
        self._attr_device_class = "power"
        self._attr_state_class = "measurement"
        self._attr_device_info = device_info
        self._attr_icon = "mdi:solar-panel"
        self._state = None

    @property
    def native_value(self):
        return self._state

    @property
    def extra_state_attributes(self):
        """Return additional state attributes."""
        module = self._coordinator.find_module(self._station_id, self._module_id)
        if module:
            attrs = {
                "module_id": module.id,
                "port": module.port,
                "position_x": module.x,
                "position_y": module.y,
            }
            if module.getLatestTime():
                attrs["last_updated"] = module.getLatestTime()
            return attrs
        return {}

    async def async_update(self):
        # Get updated system data through coordinator
        if await self._coordinator.get_system() is None:
            self._attr_available = False
            self._state = None
            return
        self._attr_available = True
        
        # Find our specific module in the updated system
        module = self._coordinator.find_module(self._station_id, self._module_id)
        if module:
            power = module.getCurrentPower()
            self._state = power if power is not None else 0
        else:
            # Module not found, set to 0
            self._state = 0


class HoymilesSolarModuleVoltageSensor(SensorEntity):
    def __init__(self, coordinator, name, station_id, module, device_info):
        self._coordinator = coordinator
        self._station_id = station_id
        self._module_id = module.id
        self._attr_name = f"{name} Voltage"
        self._attr_native_unit_of_measurement = UnitOfElectricPotential.VOLT
        self._attr_unique_id = f"hoymiles_nimbus_module_{module.id}_voltage"
        self._attr_device_class = "voltage"
        self._attr_state_class = "measurement"
        self._attr_device_info = device_info
        self._attr_icon = "mdi:flash"
        self._state = None

    @property
    def native_value(self):
        return self._state

    @property
    def extra_state_attributes(self):
        """Return additional state attributes."""
        module = self._coordinator.find_module(self._station_id, self._module_id)
        if module:
            attrs = {
                "module_id": module.id,
                "port": module.port,
                "position_x": module.x,
                "position_y": module.y,
            }
            if module.getLatestTime():
                attrs["last_updated"] = module.getLatestTime()
            return attrs
        return {}

    async def async_update(self):
        # Get updated system data through coordinator
        if await self._coordinator.get_system() is None:
            self._attr_available = False
            self._state = None
            return
        self._attr_available = True
        
        # Find our specific module in the updated system
        module = self._coordinator.find_module(self._station_id, self._module_id)
        if module:
            latest_data_point = module.getLatestDataPoint()
            if latest_data_point and latest_data_point.volt is not None:
                self._state = float(latest_data_point.volt)
            else:
                self._state = 0
        else:
            # Module not found, set to 0
            self._state = 0


class HoymilesSolarModuleCurrentSensor(SensorEntity):
    def __init__(self, coordinator, name, station_id, module, device_info):
        self._coordinator = coordinator
        self._station_id = station_id
        self._module_id = module.id
        self._attr_name = f"{name} Current"
        self._attr_native_unit_of_measurement = UnitOfElectricCurrent.AMPERE
        self._attr_unique_id = f"hoymiles_nimbus_module_{module.id}_current"
        self._attr_device_class = "current"
        self._attr_state_class = "measurement"
        self._attr_device_info = device_info
        self._attr_icon = "mdi:current-ac"
        self._state = None

    @property
    def native_value(self):
        return self._state

    @property
    def extra_state_attributes(self):
        """Return additional state attributes."""
        module = self._coordinator.find_module(self._station_id, self._module_id)
        if module:
            attrs = {
                "module_id": module.id,
                "port": module.port,
                "position_x": module.x,
                "position_y": module.y,
            }
            if module.getLatestTime():
                attrs["last_updated"] = module.getLatestTime()
            return attrs
        return {}

    async def async_update(self):
        # Get updated system data through coordinator
        if await self._coordinator.get_system() is None:
            self._attr_available = False
            self._state = None
            return
        self._attr_available = True
        
        # Find our specific module in the updated system
        module = self._coordinator.find_module(self._station_id, self._module_id)
        if module:
            latest_data_point = module.getLatestDataPoint()
            if latest_data_point and latest_data_point.ampere is not None:
                self._state = float(latest_data_point.ampere)
            else:
                self._state = 0
        else:
            # Module not found, set to 0
            self._state = 0
