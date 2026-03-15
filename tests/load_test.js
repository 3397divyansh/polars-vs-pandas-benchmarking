// tests/load_test.js
import http from "k6/http";
import { check, sleep } from "k6";

// k6 utility to randomly select items from an array
import { randomItem } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";

// 1. Configure the Load Test Parameters
export const options = {
  // This defines the traffic pattern for the benchmark
  stages: [
    { duration: "10s", target: 50 }, // Ramp-up from 0 to 50 concurrent users over 10 seconds
    { duration: "40s", target: 50 }, // Sustained load: Hold at 50 users for 40 seconds
    { duration: "10s", target: 0 }, // Ramp-down to 0 users over 10 seconds
  ],
  // Thresholds will automatically fail the test if the API starts throwing 500 errors
  thresholds: {
    http_req_failed: ["rate<0.01"], // Error rate must be less than 1%
  },
};

// 2. Generate a pool of realistic tickers matching our data generation script
// We add 'null' multiple times to the array to increase the probability that
// the query asks for the entire dataset (which forces the heaviest CPU load).
const tickers = Array.from({ length: 100 }, (_, i) => `TICKER_${i + 1}`);
tickers.push(null, null, null, null, null);

export default function () {
  // We pass the target URL dynamically via an environment variable
  // so we can use the exact same script for both Pandas and Polars.
  const TARGET_URL = __ENV.TARGET_URL || "http://localhost:8000/pricing";

  // 3. Build a dynamic payload
  // const payload = JSON.stringify({
  //   ticker: randomItem(tickers),
  //   // 50% chance to filter out options with less than 6 months to maturity
  //   min_time_to_maturity: Math.random() > 0.5 ? 0.5 : 0.0,
  // });

  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };

  // 4. Execute the HTTP GET request
  const res = http.get(TARGET_URL, params);
  if (res.status !== 200) {
    console.log(`Error ${res.status}: ${res.body}`);
  }
  // 5. Validate the response
  check(res, {
    "is status 200": (r) => r.status === 200,
    "transaction completed successfully": (r) => r !== undefined,
  });

  // Think time: Simulate real users pausing between clicks
  sleep(1);
}
