import React, { useState, useEffect, useContext } from "react";
import Grid from "@mui/material/Grid";
import moment from 'moment';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import SaveIcon from '@mui/icons-material/Save';
import * as dfd from "danfojs";
import Entries from '../../models/Entries';
import DeviceCalibration from '../../models/DeviceCalibration';
import db from '../../utils/db';
import { ResponsiveContainer } from "recharts";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import ListItem from "@mui/material/ListItem";
import Stack from "@mui/material/Stack";
import ListItemText from "@mui/material/ListItemText";
import Paper from "@mui/material/Paper";
import axios from "axios";
import NetworkCheckIcon from "@mui/icons-material/NetworkCheck";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import OutboundIcon from "@mui/icons-material/Outbound";
import ListItemAvatar from "@mui/material/ListItemAvatar";
import Avatar from "@mui/material/Avatar";
import { Line } from "react-chartjs-2";
import Layout from "../../Layout/Layout"
import { DataStore } from '../../utils/DataStore';
import { useSnackbar } from 'notistack';
import DeviceInfo from "../../components/Data/DeviceInfo";
import DatePickerComponent from "../../components/DatePickerComponent/DatePickerComponent";
import Button from "@mui/material/Button";
import { useRouter } from 'next/router'
export default function DevicePage({ tempArray, humArray, deviceCalibration }) {
  const router = useRouter()
  const { id } = router.query
  const { state } = useContext(DataStore);
  const { userInfo } = state;
  const currentDate = new Date()
  currentDate.setHours(0, 0, 0);
  useEffect(() => {
    if (!userInfo) {
      router.push('/login');
    }
    chartDataFilter()
  }, [userInfo, router]);


  const { enqueueSnackbar, closeSnackbar } = useSnackbar();
  const [startDate, SetStartDate] = useState(moment(currentDate, "YYYY-MM-DDTHH:mm:ss").format("YYYY-MM-DD"));
  const [endDate, SetEndDate] = useState(moment(currentDate, "YYYY-MM-DDTHH:mm:ss").add(1, 'days').format("YYYY-MM-DD"));
  const [current_humidity_calibration, setCurrent_humidity_calibration] = useState(deviceCalibration[0]?.humidity_calibration);
  const [current_temperature_calibration, setCurrent_temperature_calibration] = useState(deviceCalibration[0]?.temperature_calibration);

  const [tempMinArray, setTempMinArray] = useState([]);
  const [tempMaxArray, setTempMaxArray] = useState([]);
  const [tempAvgArray, setTempAvgArray] = useState([]);

  const [humMinArray, setHumMinArray] = useState([]);
  const [humMaxArray, setHumMaxArray] = useState([]);
  const [humAvgArray, setHumAvgArray] = useState([]);






  async function chartDataFilter() {
    closeSnackbar()
    try {
      const { data } = await axios.post(`${process.env.NEXT_PUBLIC_Chart_API_Python_Link}`, {
        start_date: startDate,
        end_date: endDate,
        deviceEUI: id
      });
      setTempMinArray(data.tempData.minArray)
      setTempMaxArray(data.tempData.maxArray)
      setTempAvgArray(data.tempData.avgArray)
      setHumMinArray(data.humData.minArray)
      setHumMaxArray(data.humData.maxArray)
      setHumAvgArray(data.humData.avgArray)
      setHumAvgArray(data.humData.avgArray)

      enqueueSnackbar('Filtered', { variant: 'success' });
    }
    catch (e) {
      console.log(e)
    }
  }



  async function updateCallibration() {
    closeSnackbar();
    try {
      await axios.put('/api/device-calibration/set-device-calibration', {
        temperatureCalibration: current_temperature_calibration,
        humidityCalibration: current_humidity_calibration,
        devEUI: id
      });
      enqueueSnackbar('Updated Successfully', { variant: 'success' });
    } catch (err) {
      enqueueSnackbar(err, { variant: 'error' });
    }

  }


  // TABLE DATA




  const Temperaturedata = {
    labels: ["12 AM", "1 AM", "2 AM", "3 AM", "4 AM", "5 AM", "6 AM", "7 AM", "8 AM", "9 AM", "10 AM", "11 AM", "12 PM", "1PM", "2 PM", "3 PM", "4 PM", "5 PM", "6 PM", "7 PM", "8 PM", "9 PM", "10 PM", "11 PM"],
    datasets: [
      // {
      //   label: "AC1",
      //   data: ac1Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "AC2",
      //   data: ac2Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "AC3",
      //   data: ac3Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "AC4",
      //   data: ac4Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Humidifier1",
      //   data: humidifier1Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Humidifier2",
      //   data: humidifier2Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Humidifier3",
      //   data: humidifier3Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Humidifier4",
      //   data: humidifier4Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },

      // {
      //   label: "Chillar1",
      //   data: chillar1Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Chillar2",
      //   data: chillar2Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Chillar3",
      //   data: chillar3Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Chillar4",
      //   data: chillar4Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },

      {
        label: "Min Temperature",
        data: tempMinArray,
        borderColor: [
          "red",
        ],
        borderWidth: 1,
      },
      {
        label: "Max Temperature",
        data: tempMaxArray,
        borderWidth: 1,
        borderColor: [
          "blue",
        ],

      },
      {
        label: "Average Temperature",
        data: tempAvgArray,
        borderWidth: 1,
        borderColor: [
          "black",
        ],
      },
    ],
  };

  const Humiditydata = {
    labels: ["12 AM", "1 AM", "2 AM", "3 AM", "4 AM", "5 AM", "6 AM", "7 AM", "8 AM", "9 AM", "10 AM", "11 AM", "12 PM", "1PM", "2 PM", "3 PM", "4 PM", "5 PM", "6 PM", "7 PM", "8 PM", "9 PM", "10 PM", "11 PM"],
    datasets: [
      // {
      //   label: "AC1",
      //   data: ac1Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "AC2",
      //   data: ac2Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "AC3",
      //   data: ac3Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "AC4",
      //   data: ac4Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Humidifier1",
      //   data: humidifier1Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Humidifier2",
      //   data: humidifier2Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Humidifier3",
      //   data: humidifier3Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Humidifier4",
      //   data: humidifier4Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Chillar1",
      //   data: chillar1Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Chillar2",
      //   data: chillar2Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Chillar3",
      //   data: chillar3Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      // {
      //   label: "Chillar4",
      //   data: chillar4Array,
      //   borderColor: [
      //     "green",
      //   ],
      //   borderWidth: 1,
      // },
      {
        label: "Min Humidity",
        data: humMinArray,
        borderColor: [
          "red",
        ],
        borderWidth: 1,
      },
      {
        label: "Max Humidity",
        data: humMaxArray,
        borderWidth: 1,
        borderColor: [
          "blue",
        ],

      },
      {
        label: "Average Humidity",
        data: humAvgArray,
        borderWidth: 1,
        borderColor: [
          "black",
        ],
      },
    ],
  };

  return (
    <Layout>
      <Grid style={{ backgroundColor: '#9d2eff', color: 'white' }}>
        <Typography sx={{ mb: 3 }} variant="h3" align="center" >Zone Name</Typography>
      </Grid>
      <Grid sx={{ my: 3 }} container>
        <Grid item md={5} xs={12}>
          <DeviceInfo
            deviceEUI={id}
          />
        </Grid>
        {/* CHARTS */}
        <Grid item md={7} xs={12}>
        <Stack style={{ width: '100%' }} alignItems="center"
            justifyContent="center" direction='row'>
            <DatePickerComponent
              startDate={startDate}
              SetStartDate={SetStartDate}
              endDate={endDate}
              SetEndDate={SetEndDate}
            />
            <Button onClick={() => chartDataFilter()} endIcon={<FilterAltIcon />}
              style={{ backgroundColor: '#FF5C93',borderRadius:'1rem' ,borderRadius:'1rem' , color: 'white', marginTop: '1rem', marginBottom: '1rem', padding: '0.7rem' }} >
              <b>  Click To Filter</b>
            </Button>

          </Stack>
          <Stack spacing={2}>
            <>
              <div
                style={{ border: "2px solid #9013FE", borderRadius: "1rem" }}
              >
                <ResponsiveContainer className="p-0" width="100%">
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
                      <h5>Temperature Trend</h5>
                    </div>
                    <div style={{ padding: "3px" }}>
                      <Line
                        height={150}
                        data={Temperaturedata}
                      />
                    </div>
                  </>
                </ResponsiveContainer>
              </div>
              <div
                style={{ border: "2px solid #9013FE", borderRadius: "1rem" }}
              >
                <ResponsiveContainer className="p-0" width="100%">
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
                      <h5>Humidity Trend</h5>
                    </div>
                    <div style={{ padding: "3px" }}>
                      <Line
                        height={150}
                        data={Humiditydata}
                      />
                    </div>
                  </>
                </ResponsiveContainer>
              </div>
            </>
          </Stack>
        </Grid>
      </Grid>



      {userInfo && userInfo.isAdmin == true ? (
        <Grid style={{ marginTop: "5rem" }} container spacing={4}>
          <Grid component={Paper} item sx={{ p: 3 }} md={6} xs={12}>
            <Grid sx={{ my: 3 }} container>
              <Grid item xs={12}>
                <Grid container spacing={2}>
                  <Grid item xs={4}>
                    <Typography variant="p" sx={{ fontWeight: 700 }}>
                      Temperature
                    </Typography>
                  </Grid>
                  <Grid item xs={8}>
                    <ListItem>
                      <ListItemAvatar>
                        <Avatar>
                          <ArrowDownwardIcon />
                        </Avatar>
                      </ListItemAvatar>
                      {tempArray[tempArray.length - 1] ? (
                        <ListItemText
                          primary="Input"
                          secondary={(parseFloat(tempArray[tempArray.length - 1]) -
                            parseFloat(current_temperature_calibration)).toFixed(2)}
                        />
                      ) : (
                        <ListItemText primary="Input" secondary="--" />
                      )}
                    </ListItem>
                    <ListItem>
                      <ListItemAvatar>
                        <Avatar>
                          <NetworkCheckIcon />
                        </Avatar>
                      </ListItemAvatar>
                      <TextField
                        defaultValue={current_temperature_calibration}
                        onChange={(e) => {
                          setCurrent_temperature_calibration(e.target.value);
                        }}
                        type="number"
                        fullWidth
                        id="outlined-basic"
                        label="Temperature Calibration"
                        variant="outlined"
                      />
                    </ListItem>
                    <ListItem>
                      <ListItemAvatar>
                        <Avatar>
                          <OutboundIcon />
                        </Avatar>
                      </ListItemAvatar>
                      {tempArray[tempArray.length - 1] ? (
                        <ListItemText
                          primary="Output"
                          secondary={
                            parseFloat(tempArray[tempArray.length - 1]).toFixed(2)
                          }
                        />
                      ) : (
                        <ListItemText primary="Output" secondary="--" />
                      )}
                    </ListItem>
                  </Grid>
                </Grid>
              </Grid>
            </Grid>
            <Grid item xs={12}>
              {/* Humidity */}
              <Grid container spacing={2}>
                <Grid item xs={4}>
                  <Typography variant="p" sx={{ fontWeight: 700 }}>
                    Humidity
                  </Typography>
                </Grid>
                <Grid item xs={8}>
                  <ListItem>
                    <ListItemAvatar>
                      <Avatar>
                        <ArrowDownwardIcon />
                      </Avatar>
                    </ListItemAvatar>
                    {humArray[humArray.length - 1] ? (
                      <ListItemText
                        primary="Input"
                        secondary={(parseFloat(humArray[humArray.length - 1]) - parseFloat(current_humidity_calibration)).toFixed(2)}
                      />
                    ) : (
                      <ListItemText primary="Input" secondary="--" />
                    )}
                  </ListItem>
                  <ListItem>
                    <ListItemAvatar>
                      <Avatar>
                        <NetworkCheckIcon />
                      </Avatar>
                    </ListItemAvatar>
                    <TextField
                      defaultValue={current_humidity_calibration}
                      onChange={(e) => {
                        setCurrent_humidity_calibration(e.target.value);
                      }}
                      type="number"
                      fullWidth
                      label="Humidity Calibration"
                      variant="outlined"
                    />
                  </ListItem>
                  <ListItem>
                    <ListItemAvatar>
                      <Avatar>
                        <OutboundIcon />
                      </Avatar>
                    </ListItemAvatar>
                    {humArray[humArray.length - 1] ? (
                      <ListItemText
                        primary="Output"
                        secondary={
                          parseFloat(humArray[humArray.length - 1]).toFixed(2)
                        }
                      />
                    ) : (
                      <ListItemText primary="Output" secondary="--" />
                    )}
                  </ListItem>

                  {/* <Button
                    sx={{ backgroundColor: "#38B6FF", p: 1 }}
                    onClick={() => updateCallibration()}
                    variant="contained"
                  >  Save
                  </Button> */}
                  <Button onClick={() => updateCallibration()} endIcon={<SaveIcon />}
                    style={{ width: '100%', backgroundColor: '#B8DCEA', color: '#0B70B6', marginTop: '1rem', marginBottom: '1rem', padding: '0.7rem' }} >
                    <b>   Save</b>
                  </Button>

                </Grid>

              </Grid>
            </Grid>
          </Grid>

          <Grid item lg={6} md={6} sm={6} xs={12}>

          </Grid>
        </Grid>

      ) : (<></>)}
    </Layout>
  );
}






export async function getServerSideProps(ctx) {
  const { id } = ctx.query;
  await db.connect();
  const result = await Entries.find({ devEUI: id }).lean()
  const deviceCalibration = await DeviceCalibration.find({ devEUI: id }).lean();
  await db.disconnect();

  if (result.length > 0) {
    const df = new dfd.DataFrame(result)
    const temperaturedf = df.column("temperature")
    const humiditydf = df.column("humidity")
    return {
      props: {
        tempArray: JSON.parse(JSON.stringify(temperaturedf.$data)),
        humArray: JSON.parse(JSON.stringify(humiditydf.$data)),
        deviceCalibration:  JSON.parse(JSON.stringify(deviceCalibration.map(db.convertDocToObj)))
      },
    };

  }
  else {
    return {
      props: {
        tempArray: [],
        humArray: [],
        deviceCalibration: JSON.parse(JSON.stringify(deviceCalibration.map(db.convertDocToObj)))
      },
    };
  }







}