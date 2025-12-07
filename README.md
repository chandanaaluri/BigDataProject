# 🌾 Smart Agriculture IoT System — Real-Time Monitoring & Irrigation Prediction

**Project by:** Chandana Aluri • Gayathri Netapalli • Vydehi Ramineni

---

## Overview
This project simulates IoT sensor data (soil moisture, temperature, humidity, rainfall), visualizes time-series trends, and trains a machine learning model to predict whether irrigation is needed.

The original design included Kafka + Spark Streaming, but due to Docker engine issues on the development machine, we implemented a robust local fallback architecture:
**Simulator → CSV → Jupyter Notebook (visualization + ML)**.

---

## How to run (quick)
1. Create and activate venv:
2. Install dependencies
3. Run simulator
4. Open notebook

---

## Files included
- `simulator_and_writer.py` — IoT simulator
- `agri.ipynb` — Notebook (EDA, visualization, ML)
- `requirements.txt`
- `.gitignore`

---

## Contact
For questions: Chandana / Gayathri / Vydehi
