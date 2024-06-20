# importing the packages
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime
import isodate
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
from PIL import Image

# API key coccection :

def Api_connect():
    Api_ID = "AIzaSyBMk7WfFhkePxno4rdqt7DAhdRJtzsWm5Q"
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(api_service_name, api_version, developerKey= Api_ID)
    return youtube 

youtube = Api_connect()

# Function for getting Channel information :

def get_channel_info(channel_id):

    request = youtube.channels().list(part = "snippet,ContentDetails,statistics",
                                      id = channel_id)
    response = request.execute()
    
    for i in response['items']:
        data =dict(Channel_Name = i['snippet']['title'],
                   Channel_ID = i['id'],
                   Subscribers = i['statistics']['subscriberCount'],
                   Views_Count = i['statistics']['viewCount'],
                   Video_Count = i['statistics']['videoCount'],
                   Channel_Description = i['snippet']['description'],
                   Playlist_ID = i['contentDetails']['relatedPlaylists']['uploads'] )
        
    return data    

# Function for getting Video IDs :

def get_videos_ids(channel_id):

    video_ids = []
    
    request = youtube.channels().list(id = channel_id,
                                      part = 'contentDetails')
    response = request.execute()
    
    Playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token = None
    
    while True :
        response1 = youtube.playlistItems().list(part = 'snippet',
                                                 playlistId = Playlist_ID,
                                                 maxResults = 50,
                                                 pageToken = next_page_token).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    
        next_page_token = response1.get('nextPageToken')
    
        if next_page_token is None :
            break 
     
    return video_ids

# Function for getting Vedio Informations :

def get_video_info(video_ids):

    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(part = 'snippet,contentDetails,statistics',
                                        id = video_id)
        response = request.execute()
    
        for i in response['items']:
            data = dict(Channel_Name = i['snippet']['channelTitle'],
                        Channel_ID = i['snippet']['channelId'],
                        Video_ID = i['id'],
                        Video_Title = i['snippet']['title'],
                        Thumbnail = i['snippet']['thumbnails']['default']['url'],
                        Description = i['snippet'].get('description'),
                        Published_Date = i['snippet']['publishedAt'],
                        Duration = i['contentDetails']['duration'],
                        Views_Count =i['statistics']['viewCount'],
                        Likes_Count =i['statistics'].get('likeCount'),
                        Comments_Count = i['statistics'].get('commentCount'),
                        Favourite_Count = i['statistics']['favoriteCount'],
                        Definition = i['contentDetails']['definition'],
                        Caption_Status = i['contentDetails']['caption'])
    
            video_data.append(data)

    return video_data                   

# Function for getting Comments Details :

def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids :
            request = youtube.commentThreads().list(part = 'snippet',
                                                    videoId = video_id,
                                                    maxResults = 50)
            response = request.execute()
            
            for i in response['items']:
                data = dict(Comment_ID = i['snippet']['topLevelComment']['id'],
                            Video_ID = i['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published= i['snippet']['topLevelComment']['snippet']['publishedAt'])
    
                Comment_data.append(data)
    except :
        pass
    return  Comment_data       

# Function for getting Playlist Details :

def get_playlist_details(channel_id):
    next_page_token = None
    playlist_data = []
    while True :
        request = youtube.playlists().list(part = 'snippet,contentDetails',
                                           channelId = channel_id,
                                           maxResults = 50,
                                           pageToken = next_page_token)
        response = request.execute()
        
        for i in response['items']:
            data = dict(Channel_Name = i['snippet']['channelTitle'],
                        Playlist_ID = i['id'],
                        Title = i['snippet']['title'],
                        Channel_ID = i['snippet']['channelId'],
                        Published_Date = i['snippet']['publishedAt'],
                        Video_Count = i['contentDetails']['itemCount'])
            playlist_data.append(data)  
            
        next_page_token = response.get('nextPageToken')   
        if next_page_token is None :
             break
                
    return  playlist_data

# Uploading to MongoDB:

client = pymongo.MongoClient('mongodb+srv://Ramya:Ramyamahesh@cluster0.wuc0axa.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['Youtube_Data']

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vd_ids = get_videos_ids(channel_id)
    vd_details = get_video_info(vd_ids)
    cm_details = get_comment_info(vd_ids)


    coll1 = db['channel_details']
    coll1.insert_one({'channel_information':ch_details,
                      'playlist_information': pl_details,
                      'video_information':vd_details,
                      'comment_information':cm_details})

    return "Upload completed successfully"

# Table Creation with MySQL DB :

#Channel Table :

def channels_table(single_ch_name):
    mydb = mysql.connector.connect(host = "127.0.0.1",
                                   user = "root",
                                   port = "3306",
                                   password = "Ramya$125",
                                   database = "youtube")
    cursor = mydb.cursor()
    cursor
    
    
    
    
    create_query = '''create table if not exists channels(Channel_Name varchar(255),
                                                                  Channel_ID   varchar(255) primary key,
                                                                  Subscribers  bigint,
                                                                  Views_Count   bigint,
                                                                  Video_Count  int,
                                                                  Channel_Description text,
                                                                  Playlist_ID varchar(255))'''
    cursor.execute(create_query)
    mydb.commit()
    
    
    single_ch_detail =[]
    db = client['Youtube_Data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({"channel_information.Channel_Name":single_ch_name},{'_id':0}):
        single_ch_detail.append(ch_data['channel_information'])

    df_single_channel =pd.DataFrame(single_ch_detail)
    
    
    for index,row in df_single_channel.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                               Channel_ID,
                                               Subscribers,
                                               Views_Count,
                                               Video_Count,
                                               Channel_Description,
                                               Playlist_ID)
    
                                               values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                   row['Channel_ID'],
                   row['Subscribers'],
                   row['Views_Count'],
                   row['Video_Count'],
                   row['Channel_Description'],
                   row['Playlist_ID'])
    
        try :
            cursor.execute(insert_query,values)
            mydb.commit()
    
        except :
            news = f"The Channel Name Of {single_ch_name} is Already Exists"
            return news
            
                                               
# Playlist Table:

def playlist_table(single_ch_name):
    mydb = mysql.connector.connect(host = "127.0.0.1",
                                   user = "root",
                                   port = "3306",
                                   password = "Ramya$125",
                                   database = "youtube")
    cursor = mydb.cursor()
       
    
    create_query = '''create table if not exists playlists(Channel_Name  varchar(255),
                                                           Playlist_ID varchar(255) primary key ,
                                                           Title   varchar(255),
                                                           Channel_ID varchar(255),
                                                           Published_Date DATETIME,
                                                           Video_Count int)'''
                                                           
    cursor.execute(create_query)
    mydb.commit()
    
    single_pl_list = []
    db = client['Youtube_Data']
    coll1 = db['channel_details']
    for pl_data in coll1.find({"channel_information.Channel_Name":single_ch_name},{'_id':0}):
        single_pl_list.append(pl_data['playlist_information'])
       
    df_single_playlist = pd.DataFrame(single_pl_list[0])
    
    for index,row in df_single_playlist.iterrows():
        
        insert_query = '''insert into playlists(Channel_Name,
                                               Playlist_ID,
                                               Title,
                                               Channel_ID,
                                               Published_Date,
                                               Video_Count)
    
                                               values(%s,%s,%s,%s,%s,%s)'''            
                                                           
        values = (row['Channel_Name'],
                  row['Playlist_ID'],
                  row['Title'],
                  row['Channel_ID'],
                  datetime.strptime(row['Published_Date'],'%Y-%m-%dT%H:%M:%SZ'),
                  row['Video_Count'])
                                                                    
        cursor.execute(insert_query,values)
        mydb.commit()

# Video Table:        

def video_table(single_ch_name):
    mydb = mysql.connector.connect(host = "127.0.0.1",
                                   user = "root",
                                   port = "3306",
                                   password = "Ramya$125",
                                   database = "youtube")
    cursor = mydb.cursor()
    
    
    create_query = '''create table if not exists videos(Channel_Name varchar(255),
                                                        Channel_ID varchar(255),
                                                        Video_ID varchar(255) primary key,
                                                        Video_Title varchar(255),
                                                        Thumbnail varchar(255),
                                                        Description text,
                                                        Published_Date DATETIME,
                                                        Duration int,
                                                        Views_Count bigint,
                                                        Likes_Count bigint,
                                                        Comments_Count int,
                                                        Favourite_Count int,
                                                        Definition varchar(255),
                                                        Caption_Status varchar(255))'''
                                                           
    cursor.execute(create_query)
    mydb.commit()
    
    single_vd_list = []
    db = client['Youtube_Data']
    coll1 = db['channel_details']
    for vd_data in coll1.find({"channel_information.Channel_Name":single_ch_name},{'_id':0}):
        single_vd_list.append(vd_data['video_information'])
       
    df_single_video = pd.DataFrame(single_vd_list[0])
    
    
    for index,row in df_single_video.iterrows():
    
        yt_duration = row['Duration']  # Example: YouTube duration string
        duration_obj = isodate.parse_duration(yt_duration)
        standard_time = int(duration_obj.total_seconds())  # Convert to seconds
    
        
        insert_query = '''insert into videos(Channel_Name ,
                                             Channel_ID ,
                                             Video_ID ,
                                             Video_Title ,
                                             Thumbnail ,
                                             Description ,
                                             Published_Date ,
                                             Duration ,
                                             Views_Count ,
                                             Likes_Count ,
                                             Comments_Count ,
                                             Favourite_Count ,
                                             Definition ,
                                             Caption_Status )
                                                        
                                             values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''            
                                                           
        values = (row['Channel_Name'],
                  row['Channel_ID'],
                  row['Video_ID'],
                  row['Video_Title'],
                  row['Thumbnail'],
                  row['Description'],
                  datetime.strptime(row['Published_Date'],'%Y-%m-%dT%H:%M:%SZ'),
                  standard_time, 
                  row['Views_Count'],
                  row['Likes_Count'],
                  row['Comments_Count'],
                  row['Favourite_Count'],
                  row['Definition'],
                  row['Caption_Status'])
                 
                                                                    
        cursor.execute(insert_query,values)
        mydb.commit()

# Comment Table:        

def comments_table(single_ch_name):
    
    mydb = mysql.connector.connect(host = "127.0.0.1",
                                   user = "root",
                                   port = "3306",
                                   password = "Ramya$125",
                                   database = "youtube")
    cursor = mydb.cursor()
    
    
    create_query = '''create table if not exists comments(Comment_ID varchar(255) primary key ,
                                                          Video_ID   varchar(255),
                                                          Comment_Text text,
                                                          Comment_Author  varchar(255),
                                                          Comment_Published DATETIME)'''
                                                           
    cursor.execute(create_query)
    mydb.commit()
    
    single_cm_list = []
    db = client['Youtube_Data']
    coll1 = db['channel_details']
    for cm_data in coll1.find({"channel_information.Channel_Name":single_ch_name},{'_id':0}):
        single_cm_list.append(cm_data['comment_information'])
       
    df_single_comment = pd.DataFrame(single_cm_list[0])
    
    
    for index,row in df_single_comment.iterrows():
            
            insert_query = '''insert into comments(
                                                   Comment_ID ,
                                                   Video_ID  ,
                                                   Comment_Text ,
                                                   Comment_Author ,
                                                   Comment_Published)
                                                   
                                                   values(%s,%s,%s,%s,%s)'''            
                                                               
            values = (row['Comment_ID'],
                      row['Video_ID'],
                      row['Comment_Text'],
                      row['Comment_Author'],
                      datetime.strptime(row['Comment_Published'],'%Y-%m-%dT%H:%M:%SZ'))
                      
                                                                        
            cursor.execute(insert_query,values)
            mydb.commit()

#Function for upload all data into MySQL:
 
def tables(single_channel):
    news = channels_table(single_channel)
    if news:
        return news
    else :
        playlist_table(single_channel)
        video_table(single_channel)
        comments_table(single_channel)

    return "tables created successfully" 

# Extracting Channels Information From MongoDB :

def show_channels_table():
    
    ch_list = []
    db = client['Youtube_Data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df = st.dataframe(ch_list)

    return df

# Extracting Playlist Information From MongoDB :

def show_playlists_table():
    
    pl_list = []
    db = client['Youtube_Data']
    coll1 = db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
           
    df1 = st.dataframe(pl_list)
    
    return df1

# Extracting Video Information From MongoDB :

def show_videos_table():
    
    vd_list = []
    db = client['Youtube_Data']
    coll1 = db['channel_details']
    for vd_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vd_data['video_information'])):
            vd_list.append(vd_data['video_information'][i])
    df2 = st.dataframe(vd_list)
    
    return df2

# Extracting Comments Information From MongoDB :

def show_comments_table():
    
    cm_list = []
    db = client['Youtube_Data']
    coll1 = db['channel_details']
    for cm_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(cm_data['comment_information'])):
            cm_list.append(cm_data['comment_information'][i])
           
    df3 = st.dataframe(cm_list)
    
    return df3

# streamlit :

st.set_page_config(layout = "wide")
st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
with st.sidebar:
    selected = option_menu("Main Menu", ["Home", 'Channels Info','Analysis With Questions'], 
        icons=['house', 'box','list-task'], menu_icon="cast", default_index=1)
if selected=="Home":
    col1,col2 = st.columns(2)
    with col2 :
        st.image(Image.open(r"C:\Users\ramya\Downloads\youtubelogo.jpg"),width=550)
    with col1 :   
        st.header("Overview")
        st.write("This project is focused on harvesting data from YouTube channels using the YouTube API, processing the data, and warehousing it.")
        st.write("The harvested data is initially stored in a MongoDB Atlas database as documents and is then converted into SQL records for  data analysis.")
        st.write("The project's core functionality relies on the Extract, Transform, Load (ETL) process. Features")
        st.header("Approach")
        st.write("1.Harvest YouTube channel data using the YouTube API by providing a 'Channel ID'.")
        st.write("2.Store channel data in MongoDB Atlas as documents.")
        st.write("3.Convert MongoDB data into SQL records for data analysis.")
        st.write("4.Implement Streamlit to present code and data in a user-friendly UI.")
        st.write("5.Execute data analysis using SQL queries and Python integration.")
    col1, col2 = st.columns(2)
    with col1 :
        st.header("Methods")
        st.write("1.Get YouTube Channel Data: Fetches YouTube channel data using a Channel ID and creates channel details in dictionary format.")
        st.write("2.Get Playlist Videos: Retrieves all video IDs for a provided playlist ID.")
        st.write("3.Get Video and Comment Details: Returns video and comment details for the given video IDs.")
        st.write("4.Get All Channel Details: Provides channel, video, and playlist details.")
        st.write("5.Insert Data into MongoDB: Inserts channel data into MongoDB Atlas as a document.")
        st.write("6.Get Channel Names from MongoDB: Retrieves channel names from MongoDB documents.")
        st.write("7.Convert MongoDB Document to Dataframe: Fetches MongoDB documents and converts them into dataframes for MySQL data insertion.")
        st.write("8.Data Load to SQL: Loads data into SQL.")
        st.write("9.Data Analysis: Conducts data analysis using SQL queries and Python integration.")
    with col2 :
        st.header("Tools Expertise")
        st.write("1. Python (Scripting)")
        st.write("2. Data Collection and Extraction")
        st.write("3. MongoDB")
        st.write("4. MySQL Workbench")
        st.write("5. API Integration")
    


if selected =="Channels Info":

    channel_id = st.text_input("Enter The Channel ID :")

    if st.button("Collect And Store Data", use_container_width= True):
        ch_ids = []
        db = client['Youtube_Data']
        coll1 = db['channel_details']
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data['channel_information']['Channel_ID'])

        if channel_id in ch_ids :
            st.success("Channel ID Already Exists !!!")
        else :
            insert = channel_details(channel_id)
            st.success(insert)

    all_channels=[]
    db = client['Youtube_Data']
    coll1 = db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        all_channels.append(ch_data['channel_information']['Channel_Name'])

    unique_channel = st.selectbox("Select The Channel :",all_channels)


    if st.button("Migrate To MySQL", use_container_width= True):
        Table = tables(unique_channel)
        st.success(Table)



    show_table = st.radio("Select The Table",("Channel Table","Playlist Table","Video Table","Comment Table"))

    if show_table == "Channel Table":
        show_channels_table()

    elif show_table == "Playlist Table":
        show_playlists_table()

    elif show_table == "Video Table":
        show_videos_table()

    elif show_table == "Comment Table":
        show_comments_table()
        

    

if selected=='Analysis With Questions':

    # Connection to MySQL

    mydb = mysql.connector.connect(host = "127.0.0.1",
                                    user = "root",
                                    port = "3306",
                                    password = "Ramya$125",
                                    database = "youtube")
    cursor = mydb.cursor()


    question = st.selectbox("Select The Question",("1. All The Videos And Channel Name",
                                                "2. Channels With Most Number Of Video",
                                                "3. Top 10 Most Viewed Videos",
                                                "4. Comments In Each Videos",
                                                "5. Videos With Highest Likes",
                                                "6. Total Number Of Likes Of All Videos",
                                                "7. Total Number Of Views Of Each Channel",
                                                "8. Videos Published In The Year Of 2022",
                                                "9. Average Duration Of All Videos In Each Channel",
                                                "10. Videos With Highest Number Of Comments"))
                                                
    if question =="1. All The Videos And Channel Name":
        query1 = ''' select Video_Title as Videos,Channel_Name as Channel_Name from videos'''
        cursor.execute(query1)
        t1 = cursor.fetchall()
        mydb.commit()
        df = pd.DataFrame(t1,columns=['video title','channel name'])
        st.write(df)

    elif question =="2. Channels With Most Number Of Video":
        query2 = ''' select channel_name as Channel_name, video_count as Total_Videos from channels
                    order by video_count desc'''
        cursor.execute(query2)
        t2 = cursor.fetchall()
        mydb.commit()
        df2 = pd.DataFrame(t2,columns=['Channel Name','No of Videos'])
        
        col1,col2 = st.columns(2)
        with col1:
            st.write(df2)

        with col2 :
            fig_pie_1 = px.pie( data_frame= df2, names='Channel Name', values= 'No of Videos',
                            width= 600, title = "Most Count Of Videos ", hole= 0.4 )

            st.plotly_chart(fig_pie_1)


    elif question =="3. Top 10 Most Viewed Videos":
        query3 = ''' select views_count as Views,channel_name as Channel_Name,video_title as Video_Title from videos
                    where views_count is not null  order by views desc limit 10'''
        cursor.execute(query3)
        t3 = cursor.fetchall()
        mydb.commit()
        df3 = pd.DataFrame(t3,columns=['Views','Channel_Name','Video_Title'])
        st.write(df3)

    
        fig_pie_1 = px.pie( data_frame= df3, names='Channel_Name', values = df3['Views'] ,
                        width= 600, title = "Top 10 Views", hole= 0.4 )

        st.plotly_chart(fig_pie_1)


    elif question =="4. Comments In Each Videos":
        query4 = ''' select comments_count as Comments_Count,video_title as Video_Title from videos 
                    where comments_count is not null'''
        cursor.execute(query4)
        t4 = cursor.fetchall()
        mydb.commit()
        df4 = pd.DataFrame(t4, columns=['Comments_Count','Video_Title'])
        st.write(df4)
        st.plotly_chart(px.bar(df4,x ="Video_Title",y ="Comments_Count",title= "No Of Comments In Each Videos", 
                               color_discrete_sequence= px.colors.sequential.Rainbow_r, height = 1000, width=1000,
                               hover_name="Comments_Count",hover_data="Comments_Count"))

            

    elif question =="5. Videos With Highest Likes":
        query5 = ''' select video_title as Video_Title , channel_name as Channel_Name,likes_count as Likes_Count from videos
                    where likes_count is not null order by likes_count desc'''
        cursor.execute(query5)
        t5 = cursor.fetchall()
        mydb.commit()
        df5 = pd.DataFrame(t5,columns=['Video_Title','Channel_Name','Likes_Count'])
        st.write(df5)
        st.plotly_chart(px.bar(df5,x ="Video_Title",y ="Likes_Count",title= "Total No Of Likes For Each Videos", 
                               color_discrete_sequence= px.colors.sequential.Rainbow_r, height = 1000, width= 1000,
                               hover_data= "Channel_Name",hover_name="Channel_Name"))

        
    elif question =="6. Total Number Of Likes Of All Videos":
        query6 = ''' select likes_count as Likes_Count , video_title as Video_Title from videos'''
        cursor.execute(query6)
        t6 = cursor.fetchall()
        mydb.commit()
        df6 = pd.DataFrame(t6,columns=['Likes_Count','Video_Title'])
        st.write(df6)
        st.plotly_chart(px.bar(df6, x = "Video_Title", y = "Likes_Count" ,title = "Total Number Of Likes Of All Videos", 
                         width = 1000,height = 1000, hover_data = "Likes_Count", hover_name="Likes_Count"))
            
            
    elif question =="7. Total Number Of Views Of Each Channel":
        query7 = ''' select channel_name as Channel_Name,views_count as Views_Count from channels'''
        cursor.execute(query7)
        t7 = cursor.fetchall()
        mydb.commit()
        
        df7 = pd.DataFrame(t7,columns=['Channel_Name','Views_Count'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df7)
        with col2:
            st.plotly_chart(px.line(df7, x = "Channel_Name", y = "Views_Count" ,title = "Total Number Of Views Of Channel", 
                         width = 500,height = 500, hover_data = "Views_Count", markers = True))
        
            
    elif question =="8. Videos Published In The Year Of 2022":
        query8 = ''' select video_title as Video_Title,published_date as Published_At,channel_name as Channel_Name from videos
                    where extract(year from published_date)=2022 '''
        cursor.execute(query8)
        t8 = cursor.fetchall()
        mydb.commit()
        df8 = pd.DataFrame(t8,columns=['Video_Title','Published_At','Channel_Name'])
        st.write(df8)
        st.plotly_chart(px.bar(df8,x ="Channel_Name",y ="Video_Title", title = "Videos Published In The Year Of 2022",
                               width = 1000, height= 1000, hover_name="Channel_Name"))
        col1,col2 = st.columns(2)
       


    elif question =="9. Average Duration Of All Videos In Each Channel":
        query9 = ''' select channel_name as Channel_Name , AVG (duration) as Average_Duration from videos group by channel_name '''
        cursor.execute(query9)
        t9 = cursor.fetchall()
        mydb.commit()
        df9 = pd.DataFrame(t9,columns=['Channel_Name','Average_Duration'])

        col1, col2 = st.columns(2)
        with col1 :
            st.write(df9)
        with col2 :
            st.plotly_chart(px.bar(df9,x ="Channel_Name",y ="Average_Duration", width = 500,hover_name="Average_Duration"))

    elif question =="10. Videos With Highest Number Of Comments":
        query10 = ''' select video_title as Video_Title, channel_name as Channel_Name,comments_count as Comments from videos
                    where comments_count is not null order by comments_count desc'''
        cursor.execute(query10)
        t10 = cursor.fetchall()
        mydb.commit()
        df10 = pd.DataFrame(t10,columns=['Video_Title','Channel_Name','Comments'])
        
        col1,col2 = st.columns(2)
        with col1:
            st.write(df10)
        with col2:
            st.plotly_chart(px.bar(df10, x = "Channel_Name", y = "Comments" ,title = "Videos With Highest Number Of Comments", 
                         width = 500,height = 500, hover_data = "Comments",hover_name="Video_Title"))
        
