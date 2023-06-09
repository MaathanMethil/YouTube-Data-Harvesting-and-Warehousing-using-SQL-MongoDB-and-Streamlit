import pandas as pd
import mysql.connector

# MySQL Connection
mysql_conn = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='sql@123456789',
    port=3307,
    database='mongodb01'
)

# Create a cursor object
mysql_cursor = mysql_conn.cursor()
#
def sql_q1(): # 1.what are the names of all the videos and their corresponding channels?
    sql_query01 = f"""SELECT C.Channel_name, v.Video_Name
    FROM httable.mysql_back_chtable AS c
    CROSS JOIN (
    SELECT Video_Name
    FROM httable.mysql_back_vitable
    ) AS v
    union
    SELECT C.Channel_name, v.Video_Name
    FROM httable.mysql_fire_chtable AS c
    CROSS JOIN (
    SELECT Video_Name
    FROM httable.mysql_fire_vitable
    ) AS v
    UNION
    SELECT C.Channel_name, v.Video_Name
    FROM httable.mysql_khal_chtable AS c
    CROSS JOIN (
    SELECT Video_Name
    FROM httable.mysql_khal_vitable
    ) AS v
    UNION
    SELECT C.Channel_name, v.Video_Name
    FROM httable.mysql_mone_chtable AS c
    CROSS JOIN (
    SELECT Video_Name
    FROM httable.mysql_mone_vitable
    ) AS v;
    """
    #
    mysql_cursor.execute(sql_query01)

    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q1df = pd.DataFrame(result, columns=['Channel_name', 'Video_Name'])
    # print("SQL - q1df")
    #
    return q1df
#
# ----------------------------------------

def sql_q2(): # 2.which channels have the most number of videos, and how many videos do they have?
    sql_query02 = f"""select Channel_name, Total_Videos
    from httable.mysql_back_chtable
    UNION
    select Channel_name, Total_Videos
    from httable.mysql_fire_chtable
    UNION
    select Channel_name, Total_Videos
    from httable.mysql_mone_chtable
    UNION
    select Channel_name, Total_Videos
    from httable.mysql_khal_chtable
    UNION
    select Channel_name, Total_Videos
    from httable.mysql_noma_chtable
    UNION
    select Channel_name, Total_Videos
    from httable.mysql_tami_chtable
    order by Total_Videos desc
    limit 1;
    """
    #
    mysql_cursor.execute(sql_query02)
    
    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q2df = pd.DataFrame(result, columns=['Channel_name', 'Total_Videos'])
    # print("SQL - q2df")
    #
    return q2df
#
# ----------------------------------------

def sql_q3(): # 3.what are the top 10 most viewed videos and their respective channels?
    sql_query03 = f"""SELECT v.Video_Name, v.View_Count
    FROM (
    SELECT Video_Name, View_Count
    FROM httable.mysql_back_vitable
    UNION ALL
    SELECT Video_Name, View_Count
    FROM httable.mysql_fire_vitable
    UNION ALL
    SELECT Video_Name, View_Count
    FROM httable.mysql_khal_vitable
    UNION ALL
    SELECT Video_Name, View_Count
    FROM httable.mysql_mone_vitable
    UNION ALL
    SELECT Video_Name, View_Count
    FROM httable.mysql_noma_vitable
    UNION ALL
    SELECT Video_Name, View_Count
    FROM httable.mysql_tami_vitable
    ) AS v
    ORDER BY v.View_Count DESC LIMIT 10;
    """
    #
    mysql_cursor.execute(sql_query03)
    
    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q3df = pd.DataFrame(result, columns=['Video_Name', 'View_Count'])
    # print("SQL - q3df")
    #
    return q3df
#
# ----------------------------------------

def sql_q4(): # 4.how many comments were made on each video and what'r their corresponding video names?
    sql_query04 =f"""
    SELECT c.Video_id, COUNT(c.Comment_id) AS comment_count
    FROM httable.mysql_back_cctable AS c
    JOIN httable.mysql_back_vitable AS v ON c.Video_id = v.Video_Id
    GROUP BY c.Video_id;
    """
    mysql_cursor.execute(sql_query04)
    
    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q4df = pd.DataFrame(result, columns=['Video_id', 'View_Count'])
    # print("SQL - q3df")
    #
    return q4df
#
# ----------------------------------------

def sql_q5(): # 5.which videos have heighest number of likes, and what'r their corresponding channelnames?
    sql_query05 =f"""SELECT v.Video_Id, v.Like_Count
    FROM (
    SELECT Video_Id, Like_Count
    FROM httable.mysql_back_vitable
    UNION ALL
    SELECT Video_Id, Like_Count
    FROM httable.mysql_fire_vitable
    UNION ALL
    SELECT Video_Id, Like_Count
    FROM httable.mysql_khal_vitable
    UNION ALL
    SELECT Video_Id, Like_Count
    FROM httable.mysql_mone_vitable
    UNION ALL
    SELECT Video_Id, Like_Count
    FROM httable.mysql_noma_vitable
    UNION ALL
    SELECT Video_Id, Like_Count
    FROM httable.mysql_tami_vitable
    ) AS v
    ORDER BY v.Like_Count DESC LIMIT 20;
    """
    mysql_cursor.execute(sql_query05)
    
    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q5df = pd.DataFrame(result, columns=['Video_Id', 'Like_Count'])
    # print("SQL - q3df")
    #
    return q5df
#
# ----------------------------------------

def sql_q6(): # 6.what is the total no.of likes & dislikes for each video, & what'r their corresponding videonames?
    sql_query06 = f"""select Video_name, Like_Count
    from httable.mysql_back_vitable
    order by Like_Count desc;
    """
    mysql_cursor.execute(sql_query06)
    
    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q6df = pd.DataFrame(result, columns=['Video_name', 'Like_Count'])
    # print("SQL - q3df")
    #
    return q6df
#
# ----------------------------------------

def sql_q7(): # 7.what is the total no.of views for each channels, & what'r their corresponding channelnames?
    sql_query07 = f"""select Channel_name, Views
    from httable.mysql_back_chtable
    UNION
    select Channel_name, Views
    from httable.mysql_fire_chtable
    UNION
    select Channel_name, Views
    from httable.mysql_khal_chtable
    UNION
    select Channel_name, Views
    from httable.mysql_mone_chtable
    UNION
    select Channel_name, Views
    from httable.mysql_noma_chtable
    UNION
    select Channel_name, Views
    from httable.mysql_tami_chtable;
    """
    mysql_cursor.execute(sql_query07)
    
    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q7df = pd.DataFrame(result, columns=['Channel_name', 'Views'])
    # print("SQL - q3df")
    #
    return q7df
#
# ----------------------------------------

def sql_q8(): # 8.what are the names of all the channels that have published videos in the year 2022?
    sql_query08 = f"""SELECT Video_Name, Published_At
    FROM httable.mysql_khal_vitable
    WHERE YEAR(Published_At) = 2022
    UNION
    SELECT Video_Name, Published_At
    FROM httable.mysql_noma_vitable
    WHERE YEAR(Published_At) = 2022
    LIMIT 20;
    """
    mysql_cursor.execute(sql_query08)
    
    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q8df = pd.DataFrame(result, columns=['Channel_name', 'Views'])
    # print("SQL - q3df")
    #
    return q8df
#
# ----------------------------------------

def sql_q9(): # 9.whats the average duration ofall videos in eachchannel, & what'r their corresponding channelsnames?
    sql_query09 = f"""
    SELECT Duration
    FROM httable.mysql_back_vitable
    ORDER BY Duration DESC LIMIT 10;
    """
    mysql_cursor.execute(sql_query09)
    
    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q9df = pd.DataFrame(result, columns=['Duration'])
    # print("SQL - q3df")
    #
    return q9df
    
def sql_q10(): # 10.which videos have highest no of comments, and what'r their corresponding channelnames?
    sql_query10 = f"""SELECT Video_id, count(Comment_id) AS Comment
    FROM httable.mysql_back_cctable
    Group by Video_id
    limit 10;
    """
    mysql_cursor.execute(sql_query10)
    
    # Fetch the result into a Pandas DataFrame
    result = mysql_cursor.fetchall()
    q10df = pd.DataFrame(result, columns=['Video_id', 'Comment'])
    # print("SQL - q3df")
    #
    return q10df
#
# ----------------------------------------