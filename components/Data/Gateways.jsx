import React, { useState, useEffect } from "react";
import Link from "next/link";

import Grid from "@mui/material/Grid";
var _ = require("lodash");
export default function Gateways() {
  const [Gateways, setGateways] = useState({
    totalCount: null,
    result: null
  });
  useEffect(() => {

    // POST request using fetch with set headers
    const requestOptions = {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
        'Grpc-Metadata-Authorization': 'Bearer'+' '+process.env.NEXT_PUBLIC_CHIRPSTACK_API_KEY_SECRET,
      },
    };
    fetch(`${process.env.NEXT_PUBLIC_CHIRPSTACK_URL}/api/gateways?limit=111&organizationID=${process.env.NEXT_PUBLIC_CHIRPSTACK_ORGANISATION_ID}`, requestOptions)
      .then(response => response.json())
      .then(data => setGateways({ result: data.result, totalCount: data.totalCount })).catch(function (error) {
        alert('Please Check your internet connection. Either their is no internet connection or the signals are weak');
      });
  }, [])

  var array = _.toArray(Gateways.result);
  return (
    <>
      <Grid
        container
        spacing={0}
        direction="column"
        alignItems="center"
        justifyContent="center"
        style={{ overflowX: 'scroll' }}
      >

        <table className="table">
          <thead>
            <tr>
              <th>Gateway ID</th>
              <th>Gateway Name</th>
              <th>Last Seen At</th>
            </tr>
          </thead>
          <tbody>
            {array.map((element) => {
              if (element.lastSeenAt !== null) {
                var date = new Date(element.lastSeenAt);
                var formattted_last_seen = date.toLocaleString()
              }
              else if (element.lastSeenAt === null) {
                formattted_last_seen = 'Never Seen';
              }

              return (

                <tr key={element.id}>
                     <td> 
                  <Link href={`/gateway-info/${element.id}`} style={{ color: 'black' }}><a>
                   {element.id}
                  </a></Link>
                  </td>
                  <td>
                  <Link href={`/gateway-info/${element.id}`} style={{ color: 'black' }}><a>
                       {element.name} 
                  </a></Link>
                  </td>
                  <td>
                  <Link href={`/gateway-info/${element.id}`} style={{ color: 'black' }}><a>
                     {formattted_last_seen}
                  </a></Link>
                  </td>
                </tr>

              );
            })}
          </tbody>
        </table>
      </Grid>

    </>
  );
}

