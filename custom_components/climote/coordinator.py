from datetime import timedelta
import async_timeout

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.helpers.event import async_call_later
from homeassistant.util import dt as dt_util

from .const import DOMAIN, LOGGER, DEFAULT_POLL_INTERVAL
from .api import ClimoteAPI, ClimoteConnectionError, ClimoteAuthError
from .boost_state import BoostExpectations

# A boost or cancel is an SMS to the hub, and the hub needs a while to act on it: a status read a
# second later still returns the old state. So after a command the entities show the state that was
# asked for, and one read confirms it this long afterwards.
BOOST_CONFIRM_DELAY = 60
# How long a requested "boost off" is believed without the hub agreeing. A requested "boost on" is
# believed for the boost's own duration.
BOOST_OFF_GRACE = 600


class ClimoteDataUpdateCoordinator(DataUpdateCoordinator[dict]):
    """Class to manage fetching Climote heating data from cloud."""

    def __init__(
        self,
        hass: HomeAssistant,
        api: ClimoteAPI,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL,
    ) -> None:
        """Initialize the coordinator."""
        self.api = api
        
        super().__init__(
            hass,
            LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=poll_interval_seconds),
        )
        # HA's coordinator only has a success flag, no timestamp; the Last Updated sensor reads this.
        self.last_update_success_time = None
        self._boosts = BoostExpectations()
        self._cancel_confirm = None

    def expect_boost(self, zone_id: int, on: bool, duration_hours: float = 0.0) -> None:
        """Remember that a boost on/off command was just sent, and show that state meanwhile."""
        self._boosts.expect(zone_id, on, duration_hours * 3600 if on else BOOST_OFF_GRACE)
        self.async_update_listeners()

    def expected_boost(self, zone_id: int) -> bool | None:
        """The boost state the last command asked for, or None once the hub confirmed it or it lapsed."""
        return self._boosts.get(zone_id)

    def _reconcile_expected(self, data: dict, forced: bool = False) -> None:
        confirmed, contradicted = self._boosts.reconcile(data)
        for zone_id in confirmed:
            LOGGER.info("Zone %d: hub confirms the requested boost state", zone_id)
        for zone_id, zone in contradicted:
            # A regular poll soon after a command is expected to be stale; only the read made on
            # purpose a minute later says anything about whether the hub reports this boost at all.
            (LOGGER.warning if forced else LOGGER.debug)(
                "Zone %d: a boost command was sent but the hub reports status=%r timeRemaining=%r; "
                "keeping the requested state until the boost would have ended",
                zone_id, zone.get("status"), zone.get("timeRemaining"),
            )

    def schedule_confirmation(self, delay: float = BOOST_CONFIRM_DELAY) -> None:
        """One forced hub read after `delay` seconds (one SMS), replacing any read already waiting."""
        if self._cancel_confirm is not None:
            self._cancel_confirm()

        async def _confirm(_now) -> None:
            self._cancel_confirm = None
            try:
                await self.async_force_gsm_refresh()
            except UpdateFailed as err:
                LOGGER.warning("Confirmation read after a boost command failed: %s", err)

        self._cancel_confirm = async_call_later(self.hass, delay, _confirm)

    async def async_shutdown(self) -> None:
        if self._cancel_confirm is not None:
            self._cancel_confirm()
            self._cancel_confirm = None
        await super().async_shutdown()

    async def _async_update_data(self) -> dict:
        """Fetch data from Climote Cloud (regular cached polling)."""
        LOGGER.debug("Climote coordinator polling cached state...")
        try:
            async with async_timeout.timeout(30):
                # Standard poll uses force_gsm=False to avoid expensive SMS
                data = await self.api.get_status(force_gsm=False)
                
                # Check if data is valid
                if not data or not isinstance(data, dict):
                    raise UpdateFailed("Received invalid response from Climote API")
                
                LOGGER.debug("Climote coordinator poll success: %s", data)
                self.last_update_success_time = dt_util.utcnow()
                self._reconcile_expected(data)
                return data
        except ClimoteAuthError as err:
            LOGGER.error("Climote authentication expired during polling: %s", err)
            raise UpdateFailed(f"Auth expired: {err}") from err
        except ClimoteConnectionError as err:
            LOGGER.warning("Climote connection error during polling: %s", err)
            raise UpdateFailed(f"Connection error: {err}") from err
        except Exception as err:
            LOGGER.error("Unexpected error in Climote coordinator polling: %s", err)
            raise UpdateFailed(f"Unexpected error: {err}") from err

    async def async_force_gsm_refresh(self) -> dict:
        """Trigger a real-time GSM refresh from the hub and update entities."""
        LOGGER.info("Triggering real-time GSM refresh on Climote hub...")
        try:
            # Command fresh SMS query (force_gsm=True) and wait for the hub
            data = await self.api.get_status(force_gsm=True)
            
            if data and isinstance(data, dict):
                # Update the coordinator's cached data and notify listeners immediately
                self.last_update_success_time = dt_util.utcnow()
                # Raw zone data at INFO, so what the hub says during a boost is in the normal log.
                LOGGER.info(
                    "Climote hub status after forced read: %s",
                    {k: v for k, v in data.items() if k.startswith("zone")},
                )
                self._reconcile_expected(data, forced=True)
                self.async_set_updated_data(data)
                return data
            else:
                raise UpdateFailed("Failed to fetch fresh GSM status from hub")
        except Exception as err:
            LOGGER.error("Failed to perform forced GSM update: %s", err)
            raise UpdateFailed(f"GSM refresh error: {err}") from err
