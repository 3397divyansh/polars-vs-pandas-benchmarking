// tests/load_test.js
import http from "k6/http";
import { check, sleep } from "k6";

// 1. Configure the Load Test Parameters
export const options = {
  stages: [
    { duration: "10s", target: 10 }, // Ramp-up to 50 concurrent users
    { duration: "40s", target: 10 }, // Hold steady for 40 seconds
    { duration: "10s", target: 0 },  // Cool down
  ],
  thresholds: {
    http_req_failed: ["rate<0.01"], // Error rate must be less than 1%
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

export default function () {
  // 2. Read the exact URL string from the environment variable
  const TARGET_URL = __ENV.TARGET_URL;

  // Failsafe: Ensure the URL was actually provided
  if (!TARGET_URL) {
    throw new Error("TARGET_URL environment variable is required. Example: TARGET_URL=http://localhost:8000/benchmark/heavy-join");
  }

  // 3. Execute the HTTP GET request
  const res = http.get(TARGET_URL);

  // 4. Validate the response
  check(res, {
    "is status 200": (r) => r.status === 200,
    "returned valid JSON data": (r) => {
      try {
        const data = r.json();
        // Null imputation returns an object, others return arrays
        return Array.isArray(data) || typeof data === 'object';
      } catch (e) {
        return false;
      }
    },
  });

  // Think time: Simulate real users pausing
  sleep(0.1);
}