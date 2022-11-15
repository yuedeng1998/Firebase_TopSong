"""
1 Query for songs that match user-generated criteria

For each partition of the 2022 Spotify/Tiktop top songs dataset, 
implement mapPartition(p) that takes partition p, for each song in the partition, 
check if its properties(artist, danceability of song…) match the input of the user, 
and output the title of the song if yes

Reduce function combines the titles of songs in each partition, and output the final song title list
"""

from partition_ import readPartition, getPartitionLocations


def mapPartition_spotify(file_path, partition_number, user_input):
    partition = readPartition(file_path, partition_number)
    output = []
    for song in partition:
        if user_input.get('artist') in song['artist_names'] and \
        song['peak_rank'] == user_input.get('peak_rank') and \
        float(song['danceability']) >= float(user_input.get('dance_left')) and \
        float(song['danceability']) <= float(user_input.get('dance_right')) and \
        float(song['energy']) >= float(user_input.get('energy_left')) and \
        float(song['energy']) <= float(user_input.get('energy_right')) and \
        int(song['weeks_on_chart']) >= int(user_input.get('weeks_on_chart_left')) and \
        int(song['weeks_on_chart']) <= int(user_input.get('weeks_on_chart_right')):
            output.append(song['track_name'])
    return output

def Reduce_spotify(file_path):
    addresses = getPartitionLocations(file_path)
    songs = []
    for key in addresses.keys():
        temp = mapPartition_spotify(file_path, key, user_input)
        if len(temp) == 0:
            continue
        for song in temp:
            songs.append(song)
    return songs


#Sample Spotify User input
user_input = {'artist': 'Justin Bieber', 'peak_rank': '1', 'weeks_on_chart_left': '10', \
              'weeks_on_chart_right': '1000', 'dance_left': '0.1', 'dance_right': '1', \
              'energy_left': '0', 'energy_right': '1'}

file_path = '/spotify/spotify_top_charts_22_edit.csv'
partition_number = 'p1'
map = mapPartition_spotify(file_path, partition_number, user_input)
print(map)
reduce = Reduce_spotify(file_path)
print(reduce)



def mapPartition_tiktok(file_path, partition_number, user_input):
    partition = readPartition(file_path, partition_number)
    output = []
    for song in partition:
        if user_input.get('artist') in song['artist_name'] and \
        float(song['danceability']) >= float(user_input.get('dance_left')) and \
        float(song['danceability']) <= float(user_input.get('dance_right')) and \
        float(song['energy']) >= float(user_input.get('energy_left')) and \
        float(song['energy']) <= float(user_input.get('energy_right')):
            output.append(song['track_name'])
    return output

def Reduce_tiktok(file_path):
    addresses = getPartitionLocations(file_path)
    songs = []
    for key in addresses.keys():
        temp = mapPartition_tiktok(file_path, key, user_input)
        if len(temp) == 0:
            continue
        for song in temp:
            songs.append(song)
    return songs


user_input = {'artist': 'Harry Styles', 'dance_left': '0.1', 'dance_right': '1', \
              'energy_left': '0', 'energy_right': '1'}
file_path = '/tiktok/TikTok_songs_2022_edit.csv'
partition_number = 'p1'
# print(mapPartition_tiktok(file_path, partition_number, user_input))
# print(Reduce_tiktok(file_path))