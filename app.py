# filename: app.py
# from pyspark.sql import SparkSession
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import requests
from datetime import datetime, timedelta
import plotly.graph_objects as go
import truststore
truststore.inject_into_ssl() # Handles SSL Cert exception thrown by NixtlaClient

# def get_deal_df(deal_name):
#     week_num = list(range(1, 9))
#     if deal_name == "Forever 21":
#         discount_applied = [7.99, 8.88, 10.00, 11.48, 13.55, 16.67, 22.23, 44.46]
#         total_inventory_percent_remaining = [82, 66, 51, 38, 26, 16, 8, 0]
#     else:
#         discount_applied = [8.99, 9.88, 11, 12.48, 14.55, 17.67, 23.23, 45.46]
#         total_inventory_percent_remaining = [85, 75, 65, 50, 35, 20, 5, 0]
#     return pd.DataFrame({'week_num': week_num, 
#                         'discount_applied': discount_applied, 
#                         'total_inventory_percent_remaining': total_inventory_percent_remaining})

weather_code_icons = {
    0: "☀️ Clear sky",
    1: "🌤️ Mainly clear",
    2: "⛅ Partly cloudy",
    3: "☁️ Overcast",
    45: "🌫️ Fog",
    48: "🌫️ Depositing rime fog",
    51: "🌦️ Light drizzle",
    53: "🌧️ Moderate drizzle",
    55: "🌧️ Dense drizzle",
    56: "🌧️ Light freezing drizzle",
    57: "🌧️ Dense freezing drizzle",
    61: "🌦️ Slight rain",
    63: "🌧️ Moderate rain",
    65: "🌧️ Heavy rain",
    66: "🌧️ Freezing rain (light)",
    67: "🌧️ Freezing rain (heavy)",
    71: "❄️ Light snow",
    73: "❄️ Moderate snow",
    75: "❄️ Heavy snow",
    77: "❄️ Snow grains",
    80: "🌧️ Rain showers (slight)",
    81: "🌧️ Rain showers (moderate)",
    82: "🌧️ Rain showers (violent)",
    85: "❄️ Snow showers (slight)",
    86: "❄️ Snow showers (heavy)",
    95: "⛈️ Thunderstorm (slight)",
    96: "⛈️ Thunderstorm with hail",
    99: "⛈️ Heavy thunderstorm with hail"
}

scenario1_insight = """
Event-Driven Surge – Local university graduation ceremonies are scheduled for next weekend, drawing thousands of visiting families and alumni to the area. Historic trends show strong apparel and accessory sales in the 10-day window around large events. Leverage this opportunity with aggressive early-week discounts (30-40%) to move inventory fast and capture volume ahead of weekend peak traffic.\n
Local Market Context & Recommendation:\n
•	pop ~950K with core young‐adult segment growing\n
•	60k ft² Forever 21 is one of only two fast‐fashion anchors in the mall (competitors: H&M, Zara, Shein pop‑ups)\n
•	Spring (April–May) = high traffic; start with lighter markdowns to preserve margin, then accelerate as seasonal disposal approaches summer collections\n
Weather Influence:\n
A quick weather lookup for next week is forecast to be sunny & mid‐70s°F.  Warm, dry conditions typically boost mall and outlet foot traffic, especially among 18–30 y.o. shoppers.  In Scenario 1 we keep early discounts mild (10–20%) to capture un‐discount‐averse trend‐seekers, then deepen late to clear laggards.
"""

scenario2_insight = """
Macro-Pressure Caution – Retail confidence has dipped slightly due to rising gas prices and inflationary concerns. Shopper sentiment suggests a lean-in to essentials over fast fashion splurges. Maintain minimal markdowns (5-10%) through mid-season to protect margin, monitor basket size closely, and reserve deeper cuts for consolidated clearance phases only if conversion falters.\n
Local Market Context & Recommendation:\n
•	pop ~720K; moderate-income demographic skew, price-sensitive\n
•	Local competitors reducing store hours and pushing loyalty promotions\n
•	Prior Q2 saw stronger performance with price discipline vs aggressive markdowns\n
•	Use focused promos (e.g., BOGO or category-specific deals) instead of blanket % off; emphasize quality/value messaging over urgency
"""

scenario3_insight = """
Tourism Uptick + Local Spend Caution – Regional tourism board forecasts a 15% rise in weekend visitor traffic due to a nearby music and food festival. However, local spend has shown signs of tightening due to recent utility hikes. To balance the mix, apply targeted mid-tier discounts (15-25%) on trend-forward items likely to resonate with out-of-town impulse buyers, while keeping core basics closer to full price.\n
Local Market Context & Recommendation:\n
•	pop ~850K + ~40K projected weekend visitors\n
•	Mall is 0.5 miles from festival grounds, historically sees +20% lift in foot traffic during event weekends\n
•	Competitor stores (Zara, Levi’s) typically run flash promos during events – match visibility without overcommitting margin\n
•	Focus markdowns on festival-friendly looks (e.g., denim, crop tops, accessories); reassess Monday to decide whether to deepen or hold
"""

def section4_filter(df):
    return df[['Week', 'new_discount', 'inventory_remaining%']]

def plot_section(df):
    fig, ax1 = plt.subplots(figsize = (8, 4))
    color1 = 'tab:blue'
    ax1.set_xlabel('Week Number')
    ax1.set_ylabel('Discount Applied (%)', color = color1)
    ax1.plot(df['Week'], df['new_discount'], color=color1, marker='o', label = 'Discount Applied')
    ax1.tick_params(axis='y', labelcolor=color1)

    ax2 = ax1.twinx()
    color2 = 'tab:red'
    ax2.set_ylabel('Total Inventory Left (%)', color=color2)
    ax2.bar(df['Week'], df['inventory_remaining%'], color=color2, alpha=0.6, label='Total Inventory Left(%)')
    ax2.tick_params(axis='y', labelcolor=color2)

    return fig

# def plot_predict_actual_pyplot(df):
#     fig, ax1 = plt.subplots(figsize = (10, 4))
#     x = np.arange(len(df['Week']))
#     bar_width = 0.5

#     bars1 = ax1.bar(x - bar_width/2, df['inventory_remaining%'], width=bar_width, label = "Remaining Inventory % Planning", color = '#4A90E2')
#     bars2 = ax1.bar(x + bar_width/2, df['inventory_actual_remaining%'], width=bar_width, label = 'Remaining Inventory % Actual', color = '#005C99')

#     ax1.set_ylabel('Inventory')
#     ax1.set_xlabel('Week')
#     ax1.set_xticks(x)
#     ax1.set_xticklabels(df['Week'])
#     ax1.tick_params(axis='y')
#     ax1.set_title("Weekly Inventory and Discount Overview (Planning vs. Actual)")

#     ax2 = ax1.twinx()
#     line1 = ax2.plot(x, df['new_discount'], label = "Planning Discount", color = '#7FDBFF', linewidth = 2, marker='o')
#     line2 = ax2.plot(x, df['Actual discount'], label = "Actual Discount", color = '#0074D9', linewidth = 2, marker='o')
#     ax2.set_ylabel('Discount (%)')
#     ax2.tick_params(axis='y')

#     plots_ = [bars1[0], bars2[0], line1[0], line2[0]]
#     labels_ = ['Remaining Inventory % Planning', 'Remaining Inventory % Actual', 'Planning Discount', 'Actual Discount']
#     ax1.legend(plots_, labels_, loc='upper center')

#     return fig

def plot_predict_actual_plotly(df):
    fig = go.Figure()

    # Bar chart - Inventory
    fig.add_trace(go.Bar(
        x=df['Week'],
        y=df['inventory_remaining%'],
        name='Remaining Inventory % Planning',
        marker_color='lightskyblue',
        offsetgroup=0
    ))
    fig.add_trace(go.Bar(
        x=df['Week'],
        y=df['inventory_actual_remaining%'],
        name='Remaining Inventory % Actual',
        marker_color='steelblue',
        offsetgroup=1
    ))

    # Line chart - Discount
    fig.add_trace(go.Scatter(
        x=df['Week'],
        y=df['new_discount'],
        name='Planning Discount',
        mode='lines+markers',
        marker=dict(color='deepskyblue'),
        yaxis='y2'
    ))
    fig.add_trace(go.Scatter(
        x=df['Week'],
        y=df['Actual discount'],
        name='Actual Discount',
        mode='lines+markers',
        marker=dict(color='navy'),
        yaxis='y2'
    ))

    # Layout with dual y-axes
    fig.update_layout(
        title='Weekly Inventory and Discount Overview (Planning vs. Actual)',
        xaxis=dict(title='Week'),
        yaxis=dict(title='Inventory', range=[0,100]),
        yaxis2=dict(
            title='Discount (%)',
            overlaying='y',
            side='right', range=[0,100]
        ),
        barmode='group',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template='plotly_white',
        height=500
    )

    return fig


def get_tag_value(df):
    elasticity = round(df[df['Elasticity'].notnull()].iloc[0]['Elasticity'], 2)
    initial_inventory = round(df[df['total_inventory'].notnull()].iloc[0]['total_inventory'], 2)
    projected_recovery = round(df[df['deal_recovery'].notnull()].iloc[0]['deal_recovery'], 2)
    actual_recovery = round(df[df['recovery actual'].notnull()].iloc[0]['recovery actual'], 2)
    return elasticity, initial_inventory, projected_recovery, actual_recovery

def metric_box(label, value):
    st.markdown(f"""
        <div style="
            border: 1px solid #d3d3d3;
            border-radius: 10px;
            padding: 16px;
            min-height: 80px;
            background-color: #f9f9f9;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
        ">
            <div style="font-weight: bold; font-size: 16px;">{label}</div>
            <div style="font-size: 24px; margin-top: 5px;">{value}</div>
        </div>
    """, unsafe_allow_html=True)

def llm_insights(label, value):
    st.markdown(f"""
        <div style="
            border: 1px solid #d3d3d3;
            border-radius: 10px;
            padding: 16px;
            min-height: 80px;
            background-color: #f9f9f9;
            display: flex;
            flex-direction: column;
            justify-content: left;
            align-items: left;
        ">
            <div style="font-weight: bold; font-size: 24px;">{label}</div>
            <div style="font-size: 16px; margin-top: 5px;">{value}</div>
        </div>
    """, unsafe_allow_html=True)    

def get_coordinates_from_zip(zip_code):
    geocode_url = f"https://nominatim.openstreetmap.org/search?postalcode={zip_code}&format=json"
    response = requests.get(geocode_url, headers={'User-Agent' : 'streamlit-weather-app'})
    if response.status_code ==200 and response.json():
        data = response.json()[0]
        #print(data)
        return float(data['lat']), float(data['lon'])
    else:
        print('No coordinates found!')
        return None, None

def main():
    st.set_page_config(layout='wide')
    store_info = pd.read_csv('store_info.csv')
    store_filtered_info_df = store_info.copy()

    store_data_df = pd.read_csv('store_discount_projection_info.csv')
    store_data_df['inventory_remaining%'] = 100 * store_data_df['Inventory'] / store_data_df['total_inventory']
    store_data_df['inventory_actual_remaining%'] = 100 * store_data_df['Actual Remaining Inventory'] / store_data_df['total_inventory']
    store_data_df['counter'] = store_data_df['Week'].str.extract(r'Week(\d+)').astype(int)
    store_data_df['Actual discount'] = store_data_df['Actual discount'] * 100
    store_data_df[['new_discount']] = store_data_df[['new_discount']].round(2)
    store_data_df[['inventory_remaining%']] = store_data_df[['inventory_remaining%']].round(2)
    store_data_df[['inventory_actual_remaining%']] = store_data_df[['inventory_actual_remaining%']].round(2)
    store_info['zip_code'] = store_info['store_zip_code10'].astype(str).str.zfill(5)

    col1, col2, col3 = st.columns([2, 5, 2])
    with col2:
        st.image('hilco_logo.png', caption='Hilco Discount Strategy')
    st.header("Hilco Store Discount Strategy")

    # Set dropdown list horizontally
    col1, col2, col3 = st.columns(3)

    # Get deal name list
    deal_name_lst = store_data_df['Brand_Name'].unique().tolist()
    with col1:
        deal_name = st.selectbox("Brand Name", deal_name_lst, index = 0)

    # Get store id available upon the deal name selection
    store_id_lst = store_data_df[store_data_df['Brand_Name'] == deal_name]['store_id'].unique().tolist()
    with col2:
        store_id = st.selectbox("Store Id", store_id_lst, index = 0)

    # Get week selection list
    week_id_max = store_data_df[store_data_df['store_id'] == store_id]['counter'].max()
    week_id_lst = list(range(1, week_id_max+1))
    with col3:
        current_week = st.selectbox("Current Week", week_id_lst, index = 2)

    # Get filtered dataframe
    store_filtered_info_df = store_info[store_info['store_id'] == store_id]
    store_filtered_df = store_data_df[(store_data_df['Brand_Name'] == deal_name) & (store_data_df['store_id'] == store_id) & (store_data_df['Deal_Week'] == current_week)]

    # Create weather display
    store_zip_code = store_filtered_info_df.iloc[0]['zip_code']
    lat, lon = get_coordinates_from_zip(store_zip_code)
    start_date = datetime.strptime(store_filtered_df[store_filtered_df['counter'] == current_week].iloc[0]['date1'], '%Y-%m-%d').date()
    end_date = start_date + timedelta(days=6)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date_str}&end_date={end_date_str}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode"
        f"&timezone=auto"
    )
    response = requests.get(url)
    if response.status_code ==200:
        data = response.json()
        print(data)
        daily = data['daily']
        weather_df = pd.DataFrame({
            "Date" : daily["time"],
            "Temperature Max (C)" : daily['temperature_2m_max'],
            "Temperature Min (C)" : daily['temperature_2m_min'],
            "Precipitation (mm)" : daily['precipitation_sum'],
            "Weather Code" : daily['weathercode']
        })
    else:
        print("not working!")
        print(response.status_code)

    # Add weather icons
    weather_df['Weather'] = weather_df['Weather Code'].map(weather_code_icons)
    weather_df = weather_df[['Date', 'Weather', 'Temperature Max (C)', 'Temperature Min (C)', "Precipitation (mm)"]]
    weather_df[['Temperature Max (C)', 'Temperature Min (C)', "Precipitation (mm)"]] = weather_df[['Temperature Max (C)', 'Temperature Min (C)', "Precipitation (mm)"]].round(1)

    # Display Weather
    st.subheader('🌦️Weather This Week🌦️')
    st.dataframe(weather_df)

    # Display section_1
    st.subheader("Section 1 - Store Information")
    st.markdown("---")
    store_filtered_info_df_rename = store_filtered_info_df.rename(
        columns={
            "store_id" : "Store ID",
            "store_name" : "Store Name",
            "store_zip_code10" : "Store Zip Code",
            "store_selling_sq_ft" : "Store Space sq_ft",
            'parent_company' : 'Parent Company',
            'deal_name' : 'Deal Name',
            'deal_type' : 'Deal Type',
            'sales_commence_date' : 'Sale Commence Date',
            'Length_of_Deal' : 'Deal Duration',
            'last_year_sales' : 'LY Sales',
            'population' : 'Population'
        }
    )[["Store ID", "Store Name", 'LY Sales', "Store Zip Code", "Store Space sq_ft", 'Parent Company', 'Deal Name', 'Deal Type', 'Deal Duration', 'Sale Commence Date', 'Population']]

    st.dataframe(store_filtered_info_df_rename)

    # Plot & display section_2 history and projection
    st.subheader("Section 2 - Store Discount History & Projection")
    st.markdown("---")
    # section2_fig = plot_predict_actual_pyplot(store_filtered_df)
    section2_fig = plot_predict_actual_plotly(store_filtered_df)
    store_filtered_df_display = store_filtered_df.rename(
        columns = {
        'new_discount' : "New discount",
        'week_gross' : 'Weekly gross',
        'week_gross_td' : 'Weekly gross td',
        'Inventory' : 'Inventory remaining',
        'Actual Remaining Inventory' : 'Inventory remaining actual',
        'week_nett' : 'Week net',
        'week_nett_td' : 'Week net td'
    })[['Week', 'New discount', 'Actual discount', 'Inventory remaining', 'Inventory remaining actual', 'Weekly gross', 'Weekly gross actual', 'Weekly gross td', 'Week net', 'Week net td', 'Period']]
    st.dataframe(store_filtered_df_display)
    elasticity, initial_inventory, projected_recovery, actual_recovery = get_tag_value(store_filtered_df)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        metric_box("Current Elasticity", elasticity)
    with col2:
        metric_box("Initial Total Inventory", initial_inventory)
    with col3:
        metric_box("Projected Recovery", projected_recovery)
    with col4:
        metric_box("Actual Recovery", actual_recovery)
    # st.pyplot(section2_fig, bbox_inches='tight')
    st.plotly_chart(section2_fig, use_container_width=True)

    # Plot & display section_3 LLM Other scenario
    st.subheader("Section 3 - GenAI Simulation")
    st.markdown("---")
    scenario1_df = pd.read_csv('scenario1.csv')
    scenario1_fig = plot_section(scenario1_df)
    scenario2_df = pd.read_csv('scenario2.csv')
    scenario2_fig = plot_section(scenario2_df)
    scenario3_df = pd.read_csv('scenario3.csv')
    scenario3_fig = plot_section(scenario3_df)
    tabs = st.tabs(['Scenario 1 - Aggressive Strategy', 'Scenario 2 - Medium Strategy', 'Scenario 3 - Conservative Strategy'])
    with tabs[0]:
        st.subheader("Aggressive Strategy")
        col6, col7 = st.columns(2)   
        with col6:
            st.dataframe(scenario1_df)
            llm_insights("Insights", scenario1_insight)
        with col7:
            st.pyplot(scenario1_fig)
    with tabs[1]:
        st.subheader("Medium Strategy")
        col6, col7 = st.columns(2)   
        with col6:
            st.dataframe(scenario2_df)
            llm_insights("Insights", scenario2_insight)
        with col7:
            st.pyplot(scenario2_fig)
    with tabs[2]:
        st.subheader("Conservative Strategy")
        col6, col7 = st.columns(2)   
        with col6:
            st.dataframe(scenario3_df)
            llm_insights("Insights", scenario3_insight)
        with col7:
            st.pyplot(scenario3_fig)

    # Plot & display section_4 playground
    section4_df = section4_filter(store_filtered_df)
    st.subheader("Section 4 - Discount Strategy Playground")
    st.markdown("---")
    col4, col5 = st.columns(2)
    with col4:
        section4_df_editable = st.data_editor(section4_df)
    section4_fig = plot_section(section4_df_editable)
    with col5:
        st.pyplot(section4_fig)
    
if __name__ == "__main__":
    main()