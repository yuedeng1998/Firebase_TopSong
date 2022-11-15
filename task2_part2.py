'''
2 Investigate the resurgence of nostalgic songs

mapPartition(p) take past year top songs in partition p, 
output the number of songs that appeared on Spotify or Tiktok 2022 top songs

Reduce function then combines the local counts in each partition for number of songs, 
and sums them up to produce final count
'''
# from command_edit import cat_file, readPartition, getPartitionLocations
# 直接import会报错 ValueError: Need valid data path!
import requests
def getPartitionLocations(rurl, file_path):
    file_path = file_path.replace('.', '/')
    response = requests.get(
        rurl+file_path + '.json').json()
    if not response:
        raise ValueError('No Such File!')
    data_addresses = {}
    for i in range(len(response)):
        data_addresses['p'+str(i+1)] = response['p'+str(i+1)]
    return data_addresses

def readPartition(data_url, rurl, file_path, partition_number):
    data_addresses = getPartitionLocations(rurl, file_path)
    address = data_addresses.get(partition_number)
    file_url = data_url + address
    return requests.get(file_url).json()

def cat_file(rurl, durl, file_path):
    data_addresses = getPartitionLocations(rurl, file_path)
    file_content = []
    for value in data_addresses.values():
        file_url = durl + value
        file_content.extend(requests.get(file_url).json())
    return file_content




def mapPartition_TikTok(file_path, partition_number):
    #file_path = 'pastHotSongs/songs-2_edit.csv'
    rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
    durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
    
    partition = readPartition(durl, rurl, file_path, partition_number)
    title = []
    for item in partition:
        title.append(item['title'])
    
    tiktok = cat_file(rurl, durl, '/tiktok/TikTok_songs_2022_edit.csv')
    tiktok_songs = []
    for song in tiktok:
        tiktok_songs.append(song['track_name'])
    
    rep = list(set(title).intersection(tiktok_songs))
    return len(rep)

def Reduce_Tiktok(rurl, file_path):
    addresses = getPartitionLocations(rurl, file_path)
    count = 0
    for key in addresses.keys():
        count += mapPartition_TikTok(file_path, key)
    return count

def mapPartition_Spotify(file_path, partition):
    rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
    durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
    
    partition = readPartition(durl, rurl, file_path, partition)
    title = []
    for item in partition:
        title.append(item['title'])
    
    spotify = cat_file(rurl, durl, '/spotify/spotify_top_charts_22_edit.csv')
    spotify_songs = []
    for song in spotify:
        spotify_songs.append(song['track_name'])
    
    rep = list(set(title).intersection(spotify_songs))
    return len(rep)

def Reduce_Spotify(rurl, file_path):
    addresses = getPartitionLocations(rurl, file_path)
    count = 0
    for key in addresses.keys():
        count += mapPartition_Spotify(file_path, key)
    return count



durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
file_path = '/hotSong/pastHotSongs.csv'
partition_number = 'p2'
print(mapPartition_TikTok(file_path, partition_number))
print(Reduce_Tiktok(rurl, file_path))
partition_number = 'p10'
print(mapPartition_Spotify(file_path, partition_number))
print(Reduce_Spotify(rurl, file_path))
