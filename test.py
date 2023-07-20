from datetime import datetime

# Convert the timestamp to datetime object
timestamp = '2023-06-26T07:00:30.952289+00:00'
dt = datetime.fromisoformat(timestamp)

# Convert the datetime object to local timezone
local_dt = dt.astimezone()



# Format the local time with AM/PM indicator
formatted_time = local_dt.strftime('%Y-%m-%d %I:%M:%S %p')

print("Current time in your local timezone: ", formatted_time)
