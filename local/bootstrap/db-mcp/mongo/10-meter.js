const meters = db.getSiblingDB("meter_demo");
const events = Array.from({length: 64}, (_, index) => {
  const n = index + 1;
  return {sequence_no: n, station: "station-" + (n % 4), state: n % 2 === 0 ? "ready" : "charging",
          watts: n * 10, service_note: "synthetic maintenance note"};
});
meters.meter_events.insertMany(events);
meters.createView("readings", "meter_events", [
  {$project: {_id: 0, sequence_no: 1, station: 1, state: 1, watts: 1}}
]);
meters.createRole({role: "read_meter_view", privileges: [
  {resource: {db: "meter_demo", collection: "readings"}, actions: ["find"]}
], roles: []});
meters.createUser({user: "meter_reader", pwd: process.env.METER_READER, roles: ["read_meter_view"]});
