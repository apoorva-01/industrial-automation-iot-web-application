import React, { useState, useEffect, useContext } from "react";
import Grid from "@mui/material/Grid";
import { ResponsiveContainer } from "recharts";
import { Doughnut } from "react-chartjs-2";
import Layout from "../../Layout/Layout"
import { useRouter } from 'next/router'
import { DataStore } from '../../utils/DataStore';
import GatewayInfo from "../../components/Data/GatewayInfo";
export default function GatewayPage({ }) {
  const router = useRouter()
  const { state } = useContext(DataStore);
  const { userInfo } = state;
  const { id } = router.query

  const Gatewaysdata = {
    labels: ["Off", "Active", "Never Seen"],
    datasets: [
      {
        label: "Frequency",
        data: [0, 1, 0],
        backgroundColor: [
          "rgba(255, 99, 132, 0.2)",
          "rgba(54, 162, 235, 0.2)",
          "rgba(255, 206, 86, 0.2)",
          "rgba(75, 192, 192, 0.2)",
          "rgba(153, 102, 255, 0.2)",
          "rgba(255, 159, 64, 0.2)",
        ],
        borderColor: [
          "rgba(255, 99, 132, 1)",
          "rgba(54, 162, 235, 1)",
          "rgba(255, 206, 86, 1)",
          "rgba(75, 192, 192, 1)",
          "rgba(153, 102, 255, 1)",
          "rgba(255, 159, 64, 1)",
        ],
        borderWidth: 1,
      },
    ],
  };


  useEffect(() => {
    if (!userInfo) {
      router.push('/login');
    }

  }, [userInfo, router]);





  return (
    <Layout>
      <Grid container spacing={4}>
        <Grid item xs={12} md={6}>
          <GatewayInfo gatewayID={id} />
        </Grid>
        <Grid item xs={12} md={6}>
          <ResponsiveContainer className="p-0" >
            <>
              <div
                className="p-1"
                style={{
                  backgroundColor: "#9013FE",
                  borderRadius: "1rem",
                  color: "#fff",
                  textAlign: "center",
                }}
              >
                <h5>Gateway Status</h5>
              </div>
              <Doughnut width={70} data={Gatewaysdata} />
            </>
          </ResponsiveContainer>
        </Grid>
      </Grid>
    </Layout>
  );
}


