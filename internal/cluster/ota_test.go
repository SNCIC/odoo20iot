package cluster

import "testing"

func TestOTAProgressSubjectRoundTrip(t *testing.T) {
	subject, err := OTAProgressSubject(42, "sensor.zone-1")
	if err != nil {
		t.Fatal(err)
	}
	projectID, deviceKey, err := ParseOTAProgressSubject(subject)
	if err != nil || projectID != 42 || deviceKey != "sensor.zone-1" {
		t.Fatalf("round trip mismatch: %d %q %v", projectID, deviceKey, err)
	}
	if !IsOTAProgressTopic("v1/devices/sensor.zone-1/ota/progress") || IsOTAProgressTopic("v1/devices/sensor.zone-1/ota/notify") {
		t.Fatal("OTA topic direction mismatch")
	}
}
