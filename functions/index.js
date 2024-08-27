const functions = require('firebase-functions');
const admin = require('firebase-admin');
const axios = require('axios');
const fs = require('fs');
const { promisify } = require('util');
const gunzip = promisify(require('zlib').gunzip);

admin.initializeApp();
const db = admin.firestore();

exports.updateBusArrivals = functions.https.onRequest(async (req, res) => {
    try {
        // 1. Make API call to get the download link
        const apiResponse = await axios.get('https://datamall2.mytransport.sg/ltaodataservice/v2/BusArrivalBatch', {
            headers: {
                'AccountKey': 'EPEcmrGzRWeN4824xfuvoQ=='
            }
        });

        const downloadLink = apiResponse.data.value[0].Link;

        // 2. Download the .txt.gz file
        const response = await axios.get(downloadLink, { responseType: 'arraybuffer' });
        const compressedData = response.data;

        // 3. Unzip the file
        const unzippedData = await gunzip(compressedData);
        const dataString = unzippedData.toString('utf8');

        // 4. Process the data and store in Firestore
        var busStopsProcessed = 0;

        const busStops = dataString.split('\n');
        busStops.forEach(busStopData => {
            var busStopCode = "unknown";
            const serviceBlocks = busStopData.split(';');

            serviceBlocks.forEach(function (serviceBlock, index) {
                const serviceDetails = serviceBlock.split(',');
                var counter = 0;

                if (index == 0) {
                    busStopCode = serviceDetails[counter++];
                }

                const serviceNo = serviceDetails[counter++];
                var busSequenceCounter = 0;

                console.log(`Current bus stop code: ${busStopCode} | Current service no: ${serviceNo}`);

                while (counter < serviceDetails.length) {
                    db.collection('busStops').doc(busStopCode).collection(serviceNo).doc(busSequenceCounter.toString()).set({
                        OID: serviceDetails[counter++],
                        TID: serviceDetails[counter++],
                        ETA: serviceDetails[counter++],
                        Monitored: serviceDetails[counter++],
                        Lat: serviceDetails[counter++],
                        Long: serviceDetails[counter++],
                        VisitNo: serviceDetails[counter++],
                        Load: serviceDetails[counter++],
                        Feature: serviceDetails[counter++],
                        Type: serviceDetails[counter++]
                    })

                    busSequenceCounter++;
                }
            });

            busStopsProcessed++;
        });

        res.status(200).send(`Bus arrival data updated successfully! Bus stops processed ${busStopsProcessed}`);
    } catch (error) {
        console.error('Error updating bus arrival data:', error);
        res.status(500).send('Error updating bus arrival data.');
    }
});
