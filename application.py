import streamlit as st
import pandas as pd
import numpy as np
import psycopg2

df1 = pd.read_csv("category_ref.csv")
df2 = pd.read_csv("competition_df.csv")
df3 = pd.read_csv("complexes_df.csv")
df4 = pd.read_csv("venue_df.csv")
df5 = pd.read_csv("competitor_ranking_df.csv")
df6 = pd.read_csv("competitor_df.csv")
df7 = pd.read_csv("rank_slider_df.csv")


# Table 1 : CATEGORY & COMPETITION TABLE
query_dict_1 = {
    "List all competitions along with their category name": """ SELECT COG.CATEGORY_NAME, COMP.COMPETITION_NAME
	                                                        FROM CATEGORY_OG_TABLE COG
	                                                        JOIN COMPETITION_TABLE COMP ON COG.CATEGORY_ID = COMP.CATEGORY_ID""",

    "Count the number of competitions in each category": """ SELECT COG.CATEGORY_NAME, COUNT(COG.CATEGORY_NAME) AS CATEGORY_COUNT
                                                            FROM CATEGORY_OG_TABLE COG
                                                            GROUP BY COG.CATEGORY_NAME
                                                            ORDER BY CATEGORY_COUNT DESC """,

    "Find all competitions of type 'doubles": """ SELECT COMPETITION_NAME 
                                                FROM COMPETITION_TABLE
                                                WHERE COMPETITION_NAME LIKE '%Doubles'
                                                """,

    "Get competitions that belong to a specific category (e.g., ITF Men)": """ SELECT COG.CATEGORY_NAME, COMP.COMPETITION_NAME
                                                                            FROM COMPETITION_TABLE COMP
                                                                            JOIN CATEGORY_OG_TABLE COG ON COG.CATEGORY_ID = COMP.CATEGORY_ID
                                                                            WHERE COG.CATEGORY_NAME = 'ITF Men'""",

    "Identify parent competitions and their sub-competitions": """ SELECT 
                                                                    CHILD.PARENT_ID,
                                                                    PARENT.CATEGORY_NAME AS PARENT_COMPETITION,
                                                                    CHILD.COMPETITION_ID,
                                                                    CHILD.COMPETITION_NAME AS CHILD_COMPETITION
                                                                FROM CATEGORY_OG_TABLE PARENT
                                                                JOIN COMPETITION_TABLE CHILD ON PARENT.CATEGORY_ID = CHILD.CATEGORY_ID """,

    "Analyze the distribution of competition types by category": """ SELECT C.CATEGORY_NAME, COUNT(P.COMPETITION_NAME) AS COMPETITION_COUNT, P.COMPETITION_NAME
                                                                        FROM COMPETITION_TABLE P
                                                                        LEFT JOIN CATEGORY_OG_TABLE C ON C.CATEGORY_ID = P.CATEGORY_ID
                                                                        GROUP BY C.CATEGORY_NAME , P.COMPETITION_NAME """,

    "List all competitions with no parent (top-level competitions)": """ SELECT COMPETITION_NAME 
                                                                        FROM COMPETITION_TABLE
                                                                        WHERE PARENT_ID = 'NOT AVAILABLE'"""                                                                           

}

# TABLE 2: COMPLEXES &  VENUES TABLE

query_dict_2 = {

    "List all venues along with their associated complex name" : """ 
                                                                        SELECT S.VENUE_NAME, F.COMPLEX_NAME
                                                                        FROM VENUE_TABLE S
                                                                        JOIN COMPLEXES_TABLE F ON F.COMPLEX_ID = S.COMPLEX_ID
                                                                        """,

    "Count the number of venues in each complex" : """
                                                        SELECT CWV.COMPLEX_NAME, COUNT(VENUE_NAME) AS VENUE_COUNT
                                                        FROM VENUES_WITH_COMPLEXES CWV
                                                        GROUP BY CWV.COMPLEX_NAME
                                                        ORDER BY VENUE_COUNT DESC
                                                        """,

    "Get details of venues in a specific country (e.g., Chile)" : """
                                                                    SELECT VENUE_ID, VENUE_NAME
                                                                    FROM VENUE_TABLE
                                                                    WHERE COUNTRY_NAME = 'Chile'
                                                                    """,

    "Identify all venues and their timezones" : """ SELECT VENUE_NAME, TIMEZONE
                                                    FROM VENUE_TABLE
                                                    GROUP BY VENUE_NAME, TIMEZONE
                                                    """,

    "Find complexes that have more than one venue" : """ 
                                                        SELECT F.COMPLEX_NAME, COUNT(S.VENUE_NAME) AS VENUE_COUNT
                                                        FROM COMPLEXES_TABLE F
                                                        LEFT JOIN VENUE_TABLE S ON S.COMPLEX_ID = F.COMPLEX_ID
                                                        GROUP BY F.COMPLEX_NAME
                                                        HAVING COUNT(S.VENUE_NAME) > 2
                                                        """,

    "List venues grouped by country" : """ SELECT 
                                            COUNTRY_NAME, 
                                            STRING_AGG(DISTINCT VENUE_NAME, ',' ORDER BY VENUE_NAME) AS VENUES
                                        FROM VENUE_TABLE
                                        GROUP BY COUNTRY_NAME
                                        ORDER BY COUNTRY_NAME """,

    "Find all venues for a specific complex (e.g., Nacional)" : """ SELECT S.VENUE_NAME, F.COMPLEX_NAME
                                                                    FROM VENUE_TABLE S
                                                                    JOIN COMPLEXES_TABLE F ON F.COMPLEX_ID = S.COMPLEX_ID 
                                                                    WHERE COMPLEX_NAME = 'Nacional' """
}

# TABLE 3: COMPETITOR RANKING & COMPETITORS TABLE

query_dict_3 = {

    "Get all competitors with their rank and points.": """ select f.name,s.rank,s.points 
                                                            from competitor_table f
                                                            left join competitor_ranking_table s 
                                                            on f.competitor_id = s.competitor_id
                                                            """,

    "Find competitors ranked in the top 5" : """ select * from competitor
                                                    where rank < 6 """,

    "List competitors with no rank movement (stable rank)" : """ select distinct f.name
                                                                from competitor_table f
                                                                left join competitor_ranking_table s 
                                                                on f.competitor_id = s.competitor_id
                                                                where movement = '0' """,

    "Get the total points of competitors from a specific country (e.g., Croatia)" : """ select f.country, sum (s.points) as total_points
                                                                                        from competitor_table f
                                                                                        left join competitor_ranking_table s
                                                                                        on f.competitor_id = s.competitor_id
                                                                                        where country = 'Croatia'
                                                                                        group by f.country """,

    "Count the number of competitors per country" : """ select country, count(country) as no_of_competitors
                                                        from competitor_table
                                                        group by country """
}


# CONNECTION TO DATABASE (POSTGRESQL)
def connection():
    try:
        return psycopg2.connect(
            user = "postgres",
            password = "123456",
            dbname = "Tennis DB",
            host = "localhost"
        )
    except Exception as e:
        st.error(f"Error connecting to PostgreSQL: {e}")
        return None
    
# FUNCTION TO EXECUTE QUERY AND RETURN DATAFRAME AS A RESULT
def execute_query(query, params = None):
    if params is None:
        params = ()
    conn = connection()
    if conn is None:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns = columns)
        return df
    except Exception as e:
        st.error(f"Error connecting to PostgreSQL: {e}")
        return None
    finally:
        if conn:
            conn.close()

# User Interface
st.title("🎾 Tennis Dashboard")

# SIDEBAR FILTERS
st.sidebar.header("Filter")
selected_rank2 = st.sidebar.number_input("Enter the Rank:",0,200,5)

names = df7["name"].dropna().unique()
selected_name = st.sidebar.selectbox("Name:", ["All"] + list(names))

countries = df7["country"].dropna().unique()
selected_country = st.sidebar.selectbox("Country;",["None"] + list(countries))

# Initializing filters to default on Re-load / Data on fresh opening of page.
if selected_rank2 not in st.session_state:
    st.session_state.selected_rank2 = 0

if selected_name not in st.session_state:
    st.session_state.selected_name = 'All'

if selected_country not in st.session_state:
    st.session_state.selected_country = 'None'

# Dynamic SQL Query.
query_1 = "SELECT * FROM RANK_SLIDER"
conditions = []

if selected_rank2 > 0:
    if st.sidebar.button("Apply Filter"):
        st.session_state.selected_name = 'All'
        st.session_state.selected_country = 'None'
        conditions.append(f"RANK = {selected_rank2}")

    elif selected_name != "All":
        st.session_state.selected_rank2 = 0
        st.session_state.selected_country = 'None'
        conditions.append(f"NAME = '{selected_name}'")

if selected_country != "None":
    st.session_state.selected_name = 'All'
    st.session_state.selected_rank2 = 0
    conditions.append(f"COUNTRY = '{selected_country}'")

if conditions:
    query_1 += " WHERE " + " AND ".join(conditions)

# Execute Query
try:
    output_leaderboard = execute_query(query_1) # Has dataframe in pandas

    # Layout
    col1, buff, col2 = st.columns([2,0.1,2])
   
    with col1:
        st.write("### 🥇 Leaderboard")
        st.dataframe(output_leaderboard)

        if selected_rank2:
            st.write(f"Selected rank: {selected_rank2}")

        if selected_name != "All":
            st.write(f"Selected name: {selected_name}")

        if selected_country != "All":
            st.write(f"Selected country: {selected_country}")

    with col2:
        st.subheader("🌎 Country Stats")
        query_2 = "SELECT COUNTRY,POINTS FROM RANK_SLIDER"
        output_country = execute_query(query_2)

        if "country" in output_country.columns and "points" in output_country.columns:
            st.line_chart(output_country, x="country", y="points", use_container_width=True)

except Exception as e:
    st.error(f"An error has occured: {e}")

st.subheader("📌 Category & Competition")
selected_query_1 = st.sidebar.selectbox("Choose a Query for Category:", list(query_dict_1.keys()))

if selected_query_1:
    output = execute_query(query_dict_1[selected_query_1] + " LIMIT 300")
    st.dataframe(output)

st.subheader("📌 Complexes & Venues")
selected_query_2 = st.sidebar.selectbox("Choose a Query on Venues:", list(query_dict_2.keys()))

if selected_query_2:
    output = execute_query(query_dict_2[selected_query_2] + " LIMIT 200")
    st.dataframe(output)

st.subheader("📌Competitor Rankings")
selected_query_3 = st.sidebar.selectbox("Choose a Query on Stats:", list(query_dict_3.keys()))

if selected_query_3:
    output = execute_query(query_dict_3[selected_query_3] + " LIMIT 200")
    st.dataframe(output)

