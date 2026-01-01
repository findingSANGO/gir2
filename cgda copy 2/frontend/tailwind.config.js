/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        gov: {
          50: "#f5f8ff",
          100: "#e9f0ff",
          200: "#cddcff",
          300: "#a8c0ff",
          400: "#7f9dff",
          500: "#4f74ff",
          600: "#2b54f6",
          700: "#1f3fd0",
          800: "#1d37a6",
          900: "#1b3285"
        },
        slateink: {
          50: "#f6f7f9",
          100: "#eceef2",
          200: "#d5d9e2",
          300: "#b0b8c8",
          400: "#808ca4",
          500: "#5d6a83",
          600: "#46526a",
          700: "#384357",
          800: "#2a3242",
          900: "#1f2633"
        }
      },
      boxShadow: {
        card: "0 1px 2px rgba(15, 23, 42, 0.06), 0 8px 24px rgba(15, 23, 42, 0.08)"
      }
    }
  },
  plugins: []
};


