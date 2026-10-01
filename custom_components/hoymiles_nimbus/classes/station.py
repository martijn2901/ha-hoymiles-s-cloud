import logging

_LOGGER = logging.getLogger(__name__)


class Station:
    def __init__(self, station_id, name):
        self.station_id = station_id
        self.name = name
        self.microinverters = []  # List of Microinverter objects
        self.dtus = []  # List of DTU info dictionaries

    def add_microinverter(self, microinverter):
        self.microinverters.append(microinverter)

    def set_dtus(self, dtus):
        """Set the list of DTUs for this station."""
        self.dtus = dtus

    def all_dtus_online(self):
        """Check if all DTUs are connected/online."""
        if not self.dtus:
            return False  # No DTUs means we can't determine status
        return all(dtu.get('connect', False) for dtu in self.dtus)

    def set_data(self, data):
        tree = data.get_compact()  # Store the day's data for the station
        if not isinstance(tree, list) or len(tree) < 2:
            _LOGGER.warning("Invalid day data for station ID %s", self.station_id)
            return
        id = tree[0]

        if id != self.station_id:
            _LOGGER.warning("Data station ID %s does not match Station ID %s", id, self.station_id)
            return
        
        # A newly started day has a station ID and date, but no samples yet.
        if len(tree) == 2:
            return

        for micro_data in tree[2:]:
            if not isinstance(micro_data, list) or not micro_data:
                continue
            micro_id = micro_data[0]
            micro_inverter = self.find_microinverter(micro_id)
            if not micro_inverter:
                _LOGGER.warning("Microinverter ID %s not found in station ID %s", micro_id, self.station_id)
                continue

            micro_inverter.set_data(micro_data[1:])
            
        # _LOGGER.debug("Additional tree data: %s", tree[3] if len(tree) > 3 else "None")
        # _LOGGER.debug("Setting data for station %s: %s", self.station_id, tree)

    def find_microinverter(self, micro_id):
        for micro in self.microinverters:
            if micro.id == micro_id:
                return micro
        return None
    
    def __repr__(self):
        return f"Station(id={self.station_id}, name={self.name}, microinverters={len(self.microinverters)})"
