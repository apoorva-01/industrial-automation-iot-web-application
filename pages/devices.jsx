import React,{useContext,useEffect} from "react";
import Grid from "@mui/material/Grid";
import Typography from "@mui/material/Typography";
// components
import Devices from "../components/Data/Devices";
import Layout from "../Layout/Layout"
import { DataStore } from '../utils/DataStore';
import { useRouter } from 'next/router';
export default function DevicePage() {
  const { state} = useContext(DataStore);
  const { userInfo } = state;
  const router = useRouter();
  useEffect(() => {
    if (!userInfo) {
      router.push('/login');

    }
  }, [userInfo,router]);
  return (
    <Layout>
     <Typography sx={{ mb: 3 }} variant="h4"  >Devices</Typography>
     <Devices/>
      </Layout>
  );
}
