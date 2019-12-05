const schedule = require('node-schedule');
const fetch = require("node-fetch");
const { Connection, query } = require("stardog");
const config = require('./config').config;

const conn = new Connection({
  username: "admin",
  password: "admin",
  endpoint: "http://localhost:5820"
});

schedule.scheduleJob('* * * * *', async function () {
  console.log('Starting Update!');
  
  let promises = [];
  for (let i = 0; i < config.length; i++) {
    promises.push(fetch_data(config[i]));
  }
  let answer = await Promise.all(promises);
  let data = ''
  for (let stand of answer[0]) {
    if (stand.id && stand.lat && stand.lng && stand.name && stand.nb_bike_stands) {
      data += `
      :${stand.id} a :BikeStation ;
        :name '${escape(stand.name)}' ;
        :lat '${stand.lat}' ;
        :lng '${stand.lng}' ;
        :nb_bike_stands '${stand.nb_bike_stands}' ;
        :city '${stand.city}' .`;
    }
    console.log(stand.name)
  }
  await insert_data(data);
});

async function fetch_data(_config) {
  return await fetch(_config.link)
    .then(response => {
      return response.json()
    })
    .then(data => {
      return new Promise(resolve => {
        let result = [];
        for (let elem of data[_config.path]) {
          result.push({
            id: _config.city + elem[_config.station.path][_config.station.id],
            name: elem[_config.station.path][_config.station.name],
            lat: elem[_config.station.path][_config.station.lat],
            lng: elem[_config.station.path][_config.station.lng],
            nb_bike_stands: elem[_config.station.path][_config.station.nb_bike_stands],
            city: _config.city
          });
        }
        resolve(result);
      })
    })
    .catch(err => {
      console.log('Error, cannot fetch data ' + err)
    })
}

async function insert_data(_data) {
  const q = `PREFIX :<http://example.org/> 
  insert data { ${_data} }`;
  query.execute(conn, 'opencycles', q)
    .then(result => {
      console.log(result);
    })
    .catch(err => {
      console.log(err);
    })
}
