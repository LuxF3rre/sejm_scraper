# Sejm Scraper

```console
docker-compose up
```

```powershell
python scrape_votings.py | Tee-Object -FilePath "votings.log" -Append
```

```powershell
python scrape_party_links.py | Tee-Object -FilePath "party_links.log" -Append
```

```powershell
python scrape_votes.py | Tee-Object -FilePath "votes.log" -Append
```
