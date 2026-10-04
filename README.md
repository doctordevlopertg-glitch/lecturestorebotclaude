# Lecture Bot

## Setup
1. Create the bot with @BotFather and copy the token.
2. Create a private channel, add the bot as **admin**, and upload your lectures there.
   Channel id looks like `-100xxxxxxxxxx`. Message id = the number at the end of a post link.
3. Create a free MongoDB Atlas cluster and copy the connection string
   (allow access from 0.0.0.0/0 so Heroku can connect).

## Deploy (GitHub → Heroku)
1. Push this folder to a GitHub repo.
2. Heroku → New app → Deploy → connect GitHub → Deploy Branch.
3. Settings → Config Vars: add everything from `.env.example`.
4. Resources tab: turn **worker** ON (turn **web** OFF if it appears).

## Commands
- `/start` – course list → chapters → lectures (auto-deleted after AUTO_DELETE_HOURS, default 3)
- `/addbatch` (admin) – asks course, chapter, first message id, last message id
- `/broadcast` (admin) – reply to any message to send it to all users
- `/stats` (admin) – users, courses, chapters, pending deletions
