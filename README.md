# Stock Screener: Fundamental Analysis Pre-Filter with Python, Dask, and yfinance

**Quickly Evaluate Stocks and Weed Out Hype: A First Step in Fundamental Analysis**

This Python stock screener uses Dask and yfinance to help you *quickly evaluate* stocks based on their *business fundamentals*. It's designed as a *preliminary filter* to identify companies with potentially sound financial footing, *separating them from those driven by hype or fleeting trends*. This is *not* an investment strategy itself, but a tool to *save you time* by prioritizing stocks worthy of *further, in-depth* research. Ideal for investors who want a data-driven way to assess the initial quality of potential investments.

**Key Features:**

*   **Fast, Parallel Processing:** Uses Dask for efficient parallel processing, quickly analyzing large lists of stocks.
*   **Fundamental Analysis Filtering:** Calculates essential metrics to assess basic financial health:
    *   Compound Annual Growth Rate (CAGR)
    *   Sharpe Ratio
    *   Earnings Per Share (EPS)
    *   Price-to-Earnings (P/E) Ratio
    *   Return on Equity (ROE)
    *   Enterprise Value / EBITDA (EV/EBITDA)
*   **Customizable Criteria:** Adjust parameters in the `Config` class to match your own definition of "sound fundamentals." Define thresholds for CAGR, EPS, Sharpe Ratio, price range, P/E, ROE, and EV/EBITDA.
*   **Flexible Data Source:** Uses `yfinance` to pull data from Yahoo Finance.
*   **Organized Output:** Exports *potentially* qualified stocks to an Excel spreadsheet (`qualified_stocks.xlsx`).
*   **Robust Error Handling:** Includes retries and error handling for data inconsistencies.
*   **Modern Python Best Practices:**  Built with Type Hints, Pydantic, ABCs, and Context Managers. Uses a pyproject.toml approach.

**How It Works: A Fundamental Pre-Screen**

This tool is designed to be a *first-pass filter*, helping you quickly eliminate stocks that *don't* meet basic fundamental criteria. It's a way to separate the wheat from the chaff *before* you invest significant time in research. Here's the process:

1.  **Data Acquisition:** Fetches historical price data and financial info from Yahoo Finance for each stock symbol in your `all_tickers.txt` file.
2.  **Metric Calculation:** Calculates the fundamental metrics listed above.
3.  **Strict Filtering:** Applies your *custom* criteria (defined in `Config`).  Only stocks that meet *all* of your thresholds are passed through. This is a *conservative* filter by design.
4.  **Export:** The stocks that pass the filter, along with their metrics, are exported to an Excel file.

**Why Use This Screener?**

This tool is *not* a stock recommendation engine. It's a *time-saving pre-filter*.  It helps you:

*   **Avoid Hype:** Quickly identify and discard stocks that might be popular in the news or on social media, but *lack* strong financial fundamentals.
*   **Focus Your Research:**  Narrow down your list of potential investments to companies that, at a minimum, show signs of financial stability and reasonable valuation.
*   **Apply Your Own Criteria:** Define *your* definition of "fundamental soundness."
*   **Start Your Due Diligence:**  The output is a *starting point*, *not* the end of your research. You *must* conduct further, in-depth analysis before making *any* investment decisions.

**Getting Started:**

1.  Ensure `uv` installed (if using):
    ```bash
    pip install uv
    uv venv
    . .venv/bin/activate
    ```
2.  Create a 'pyproject.toml' file; ensure proper configurations and at a minimum, place:
     ```toml
     [build-system]
     requires = ["hatchling"]
     build-backend = "hatchling.build"
     [project]
     name = "stock_analyzer"  # Choose a suitable project name
     version = "0.1.0"
     description = "Analyzes stocks based on financial data."
     dependencies = [
       "dask[distributed]",
       "numpy",
       "yfinance",
       "openpyxl",
       "pydantic",
       "tqdm",
       "modin[dask]",
     ]
        ```
    Install:
       ```bash
       uv pip install -r requirements.txt
      ```

3.  **Create `all_tickers.txt`:**  List each stock symbol (e.g., AAPL, MSFT, GOOG) on a separate line.
4.  **Configure (Optional):** Adjust the `Config` class in the Python script.
5.  **Run the Script:** `python your_script_name.py`
6.  **Review Results:** Examine `qualified_stocks.xlsx`. Remember, this is *only* a preliminary list.

**Disclaimer:** This tool is for informational purposes only and *does not* constitute financial advice. Investing involves risk. Past performance is not indicative of future results. *Always perform thorough, independent research before making any investment decisions.*

---
