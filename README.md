# ready_for_moderation
This project is about for notifying moderators (people who moderate data from photos) about uploaded photos on server.

Input data: throgh connection to database it's getting the information about number of uploaded photos per task per wave (kind of 
plan/project of collection data due to requirements, usually lasts 1-2 days).

Logic: having known a plan per task per wave and checking a fact from database it is calculating points about current status of collection and give alert 
if there are some ready photos for moderation.

Output: notifications in telegram channel about new photos which are ready for moderation

Project benefits: it allows moderators to be aware of new bunch of photo to moderate. In particular, working in hot deadlines it is substantial
to moderate data asap
