'''
2 Investigate the resurgence of nostalgic songs

mapPartition(p) take past year top songs in partition p, 
output the number of songs that appeared on Spotify or Tiktok 2022 top songs

Reduce function then combines the local counts in each partition for number of songs, 
and sums them up to produce final count
'''


def mapPartition_TikTok(file_path, partition):
    #file_path = 'songs-2_edit.json'
    rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
    durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
    
    partition = readPartition(file_path, partition)
    tiktok = cat_file(rurl, durl, 'tiktok/TikTok_songs_2022_edit.json')
    
    title = []
    for item in partition:
        title.append(item['title'])
    
    #cat_file返回的是dic吗
    tiktok_songs = []
    for key, value in tiktok.items():
        tiktok_songs.append(value['track_name'])
    
    rep = list(set(title).intersection(tiktok_songs))
    return len(rep)

def Reduce_Tiktok(rurl, file_path):
    addresses = getPartitionLocations(rurl, file_path)
    count = 0
    for key in addresses.keys():
        count += mapPartition_TikTok(file_path, key)
    return count

def mapPartition_Spotify(file_path, partition):
    #file_path = 'songs-2_edit.json'
    rurl = 'https://project-dc1b5-default-rtdb.firebaseio.com/root'
    durl = 'https://project-dc1b5-default-rtdb.firebaseio.com/data'
    
    partition = readPartition(file_path, partition)
    spotify = cat_file(rurl, durl, 'spotify/spotify_top_charts_22_edit.json')
    
    title = []
    for item in partition:
        title.append(item['title'])
    
    #cat_file返回的是dic吗
    spotify_songs = []
    for key, value in spotify.items():
        spotify_songs.append(value['track_name'])
    
    rep = list(set(title).intersection(spotify_songs))
    return len(rep)

def Reduce_Spotify(rurl, file_path):
    addresses = getPartitionLocations(rurl, file_path)
    count = 0
    for key in addresses.keys():
        count += mapPartition_Spotify(file_path, key)
    return count