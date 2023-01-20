import React,{useContext,useEffect} from "react";
import Typography from "@mui/material/Typography";
import Grid from "@mui/material/Grid";
// components
import Gateways from "../components/Data/Gateways";
import Layout from "../Layout/Layout"
import { DataStore } from '../utils/DataStore';
import { useRouter } from 'next/router';
export default function DevicePage() {
  const { state } = useContext(DataStore);
  const { userInfo } = state;
  const router = useRouter();
  useEffect(() => {
    if (!userInfo) {
      router.push('/login');
    }
  }, [userInfo,router]);
  return (
    <Layout>
     <Typography sx={{ mb: 3 }} variant="h4">Gateways</Typography>
      <Grid container spacing={4}>
        <Grid  item xs={12}>
          <Gateways/>
        </Grid>
      </Grid>
      </Layout>
  );
}
