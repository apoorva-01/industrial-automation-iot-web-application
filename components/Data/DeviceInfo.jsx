import React, { useState, useEffect } from "react";
import Grid from "@mui/material/Grid";
import Paper from "@mui/material/Paper";
export default function DeviceInfo(props) {
  const [DeviceInfo, setDeviceInfo] = useState({
    devEUI: null,
    applicationID: null,
    variables: null,
    tags: null,
    description: null,
    deviceProfileID: null,
    isDisabled: null,
    referenceAltitude: null,
    name: null,
    skipFCntCheck: null,
    lastSeenAt: null,
    location: null,
    deviceStatusMargin: null,
    deviceStatusBattery: null,
  });
  useEffect(() => {

    const requestOptions = {
      method: "GET",
      headers: {
        Accept: "application/json",
        "Grpc-Metadata-Authorization":
        'Bearer'+' '+process.env.NEXT_PUBLIC_CHIRPSTACK_API_KEY_SECRET,
      },
    };
    fetch(
      `${process.env.NEXT_PUBLIC_CHIRPSTACK_URL}/api/devices/${props.deviceEUI}`,
      requestOptions,
    )
      .then((response) => response.json())
      .then((data) =>
        setDeviceInfo({
          devEUI: data.device.devEUI,
          tags: data.device.tags,
          variables: data.device.variables,
          skipFCntCheck: data.device.skipFCntCheck,
          referenceAltitude: data.device.referenceAltitude,
          name: data.device.name,
          deviceProfileID: data.device.deviceProfileID,
          description: data.device.description,
          applicationID: data.device.applicationID,
          deviceStatusMargin: data.deviceStatusMargin,
          deviceStatusBattery: data.deviceStatusBattery,
          location: data.location,
          lastSeenAt: data.lastSeenAt,
        }),
      ).catch(function (error) {
        alert('Please Check your internet connection. Either their is no internet connection or the signals are weak');
      });
  }, [props.deviceEUI]);


  var date = new Date(DeviceInfo.lastSeenAt);
  var formattted_last_seen = date.toLocaleString()

  return (
    <>
      <Grid
        container
        spacing={0}
        direction="column"
        alignItems="center"
        justifyContent="center"
      >
        <Paper sx={{padding:3}}>
       <table className="table table-striped  table-hover">
       <thead >
            <tr>
              <th>Parameter</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Device EUI</td>
              <td>{DeviceInfo.devEUI}</td>
            </tr>
            <tr>
              <td>Device Name</td>
              <td>{DeviceInfo.name}</td>
            </tr>
            <tr>
              <td>Last Seen</td>
              <td>{formattted_last_seen}</td>
            </tr>
            <tr>
              <td>Device Description</td>
              <td>{DeviceInfo.description}</td>
            </tr>
            <tr>
              <td>Device Status Margin</td>
              <td>{DeviceInfo.deviceStatusMargin}</td>
            </tr>
            <tr>
              <td>Device Status Battery</td>
              <td>{DeviceInfo.deviceStatusBattery}</td>
            </tr>
            <tr>
              <td>Device SkipFCntCheck</td>
              <td>{DeviceInfo.skipFCntCheck}</td>
            </tr>
            <tr>
              <td>Device Refrence Altitude</td>
              <td>{DeviceInfo.referenceAltitude}</td>
            </tr>
            <tr>
              <td>Device Location</td>
              <td>{DeviceInfo.location}</td>
            </tr>
          </tbody>
        </table></Paper>
      </Grid>
    </>
  );
}
