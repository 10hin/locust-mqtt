import paho.mqtt.client as mqtt
import time
import threading


class MqttClient(mqtt.Client):
    def __init__(self, request_event, client_id="", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp"):
        super().__init__(client_id, clean_session, userdata, protocol, transport)

        self._sessions_mutex = threading.RLock()
        self._sessions = {}
        self._request_event = request_event
        self._custom_on_publish_mutex = threading.RLock()
        self._custom_on_publish = None

    def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        request_meta = {
            "request_type": "mqtt",
            "name": f"publish(\"{topic}\")",
            "start_time": time.time(),
            "response_length": 0,
            "response": None,
            "context": {},
            "exception": None,
        }
        start_perf_counter = time.perf_counter()

        with self._sessions_mutex:
            msg_info = super().publish(topic, payload, qos, retain, properties)

            mid = msg_info.mid
            rc = msg_info.rc
            if rc == mqtt.MQTT_ERR_SUCCESS:
                self._sessions[mid] = _MqttClientSession(mid, request_meta, start_perf_counter)

                return msg_info

        request_meta["response_time"] = (time.perf_counter() - start_perf_counter) * 1000
        if rc == mqtt.MQTT_ERR_NO_CONN:
            request_meta["exception"] = "This client not currently connected"
        elif rc == mqtt.MQTT_ERR_QUEUE_SIZE:
            request_meta["exception"] = "Message queue is full"
        else:
            request_meta["exception"] = "Unknown error happens and failed to send message"
        self._request_event.fire(**request_meta)
        return msg_info

    @property
    def on_publish(self):
        return self._custom_on_publish

    @on_publish.setter
    def on_publish(self, func):
        def wrapped(client, userdata, mid):
            end_perf_counter = time.perf_counter()
            with self._sessions_mutex:
                session = self._sessions.pop(mid)
                request_meta = session.request_meta
                start_perf_counter = session.start_perf_counter
            request_meta["response_time"] = (end_perf_counter - start_perf_counter) * 1_000
            self._request_event.fire(**request_meta)

            func(client, userdata, mid)

        with self._custom_on_publish_mutex:
            self._custom_on_publish = func
            super().on_publish = wrapped


class _MqttClientSession:
    def __init__(self, mid, request_meta, start_perf_counter):
        self.mid = mid
        self.request_meta = request_meta
        self.start_perf_counter = start_perf_counter
