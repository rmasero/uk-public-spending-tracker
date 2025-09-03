import streamlit as st
from council_fetchers import FETCHERS

st.title("UK Public Spending Tracker")
st.write("View and compare payments from various UK councils.")

if st.button("Fetch all council payments"):
    progress_bar = st.progress(0)
    council_payments = {}
    councils = list(FETCHERS.keys())
    for i, (council, fetch_func) in enumerate(FETCHERS.items()):
        try:
            payments = fetch_func()
            council_payments[council] = payments
            st.success(f"Loaded {len(payments)} payments from {council}")
        except Exception as e:
            st.warning(f"Failed to fetch payments for {council}: {e}")
        progress_bar.progress((i + 1) / len(councils))
    progress_bar.empty()

    # Optionally, display data for the user to browse:
    council_selected = st.selectbox("Select a council to view payments", list(council_payments.keys()))
    if council_selected:
        payments = council_payments[council_selected]
        st.write(f"Showing {len(payments)} payments from {council_selected}:")
        st.dataframe(payments)

    # If you want to allow CSV download:
    import pandas as pd
    if council_selected and council_payments[council_selected]:
        df = pd.DataFrame(council_payments[council_selected])
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download as CSV",
            data=csv,
            file_name=f"{council_selected}_payments.csv",
            mime="text/csv",
        )
else:
    st.info("Click the button above to fetch payments from all councils.")

st.markdown("Data is fetched live from each council's open data portal using custom fetchers.")
