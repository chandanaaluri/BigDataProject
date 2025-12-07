{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "0a1617f0-11bc-44ac-b07f-6ff9cf420156",
   "metadata": {},
   "outputs": [],
   "source": [
    "from flask import Flask, render_template, send_file\n",
    "# your other imports (e.g. from dashboard_visuals import generate_soil_plot)\n",
    "\n",
    "app = Flask(__name__)\n",
    "\n",
    "@app.route('/')\n",
    "def home():\n",
    "    # call your plotting function here if needed, e.g.\n",
    "    # generate_soil_plot()  # must save to plots/avg_soil_moisture.png\n",
    "    return render_template('index.html')  # or return simple HTML\n",
    "\n",
    "@app.route('/plot')\n",
    "def plot():\n",
    "    return send_file('plots/avg_soil_moisture.png', mimetype='image/png')\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    app.run(debug=True)\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.13.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
