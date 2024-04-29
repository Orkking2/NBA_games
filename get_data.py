import time

GENISIS = time.perf_counter()

import pandas as pd
from threading import Thread
from multiprocessing.pool import ThreadPool as Pool

P = Pool()

from nba_api.stats.endpoints import (leaguegamefinder, commonteamroster, commonplayoffseries, leaguedashteamstats, boxscoresummaryv2, leaguestandingsv3)

import requests
import re
import atomics

from typing import *
import itertools
from copy import copy
from tqdm import tqdm


def default(possibly_none, default):
  return possibly_none if possibly_none is not None else default

def retry_exceptions(f, on_fail = None):
  def wrapped(*args, **kwargs): 
    while(True):
      time.sleep(.1)
      try: return f(*args, **kwargs)
      except: 
        if on_fail is not None: on_fail()
  return wrapped

class UniTPIMapIt:
  def __init__(self, func, iterable):
    self.iterable = copy(iter(iterable))
    self.func = func
  def __iter__(self):
    return self
  def __next__(self):
    return self.func(self.iterable.__next__())

class UniThreadPool:
  def map(self, func, iterable, chunksize = None):
    return [func(e) for e in iterable]
  def imap(self, func, iterable, chunksize = None):
    return UniTPIMapIt(func=func, iterable=iterable)

def tqdm_imap(pool, func, iterable, **kwargs):
  return tqdm(pool.imap(func, iterable), total=len(iterable), **kwargs)

class ProxyIps:
  """Class to hold proxy ip addresses"""
  def __init__(
    self, 
    needed_passes: int | None = None, 
    chances: int | None = None, 
    pool: Pool | UniThreadPool = UniThreadPool(), 
    formatter: Callable[[requests.Response], List[str]] | None = None,
    sources: List[str] | None = None,
    ip_regex: str | None = None,
    ips: List[str] | None = None,
    no_print: bool = False
  ):
    self.passes = default(needed_passes, 1)
    self.chances = default(chances, 1)
    self.pool = pool
    self.ips = set(default(ips, []))
    self.valid_ips = list([None])
    self.ip_regex = default(ip_regex, r"\d+.\d+.\d+.\d+:\d\d+")
    self.formatter = default(formatter, lambda resp: re.findall(self.ip_regex, resp.text))
    self.sources = [(url, self.formatter) for url in default(sources, [])]
    self.proxy_counter = atomics.atomic(width=4, atype=atomics.UINT)
    self.no_print = no_print
    self.thread_bool = True

  def reset_pool(
    self,
    pool: Pool | UniThreadPool = UniThreadPool()
  ):
    self.pool = pool

  def add_ips(
    self, 
    ips: List[str]
  ):
    self.ips.union(ips)
    return self
  
  def add_source(
    self, 
    url: str, 
    formatter: Callable[[requests.Response], List[str]] | None = None
  ):
    self.sources.append((url, default(formatter, self.formatter)))
    return self
  
  def get_ips_from_sources(
    self,
    range: slice | None = None
  ):
    self.ips.update(itertools.chain.from_iterable(
      self.pool.imap(
        func=lambda source: source[1](retry_exceptions(requests.get)(source[0])),
        iterable=self.sources[default(range, slice(0, len(self.sources)))]
      ) if self.no_print else tqdm_imap(
        pool=self.pool,
        func=lambda source: source[1](retry_exceptions(requests.get)(source[0])),
        iterable=self.sources[default(range, slice(0, len(self.sources)))],
        desc="Collecting ips"
      )
    ))
    return self

  def get_valid_ips(
    self,
  ):
    def func(ip):
      chances_used = 0
      passes_used = 0
      while self.thread_bool:
        try:
          while passes_used < self.passes:
            leaguestandingsv3.LeagueStandingsV3(
              proxy=ip
            ).get_data_frames()[0]
            passes_used += 1
          return ip
        except:
          chances_used += 1
          if chances_used >= self.chances:
            return None
      return None
    self.valid_ips = list(
      set(self.valid_ips).union(
        self.pool.imap(
          func=func,
          iterable=self.ips - set(self.valid_ips)
        ) if self.no_print else tqdm_imap(
          pool=self.pool, 
          func=func, 
          iterable=self.ips - set(self.valid_ips),
          desc="Validating ips"
        )
      )
    )
    return self

  def get_proxy(self):
    return self.valid_ips[self.proxy_counter.fetch_inc() % len(self.valid_ips)]
  
  def begin_background_ip_checking(self):
    self.reset_pool()
    self.thread_bool = True
    self.no_print = True
    def f():
      while self.thread_bool:
        retry_exceptions(self.get_ips_from_sources().get_valid_ips)()
    self.thread = Thread(
      target=f
    )
    self.thread.start()

  def end_background_thread(self):
    self.thread_bool = False
    #self.thread.join()

ips = ProxyIps(
  pool=P,
  needed_passes=3,
  chances=1,
).add_source(
  url="https://proxylist.geonode.com/api/proxy-list?protocols=http%2Chttps&limit=500&page=1&sort_by=lastChecked&sort_type=desc",
  formatter=lambda resp: [f"{e["ip"]}:{e["port"]}" for e in resp.json()["data"]]
).add_source(
  url="https://spys.me/proxy.txt"
).add_source(
  url="https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
).get_ips_from_sources().get_valid_ips()

print(f"Found {len(ips.valid_ips)} valid ips")

print("Starting background ip checker...")
ips.begin_background_ip_checking()

print("teams.csv updating...")
epoch = time.perf_counter()

leaguedf = leaguedashteamstats.LeagueDashTeamStats()\
  .get_data_frames()[0]\
  .join(
    leaguestandingsv3.LeagueStandingsV3()\
      .get_data_frames()[0]\
      .rename(columns={"TeamID": "TEAM_ID", "LeagueID": "LEAGUE_ID"})\
      .set_index("TEAM_ID"), 
    on="TEAM_ID"
  )
leaguedf.to_csv("teams.csv", index=False)
print(f"teams.csv updated -\t{time.perf_counter() - epoch}s")

def count(f, counter, total, name):
  def wrapped(id):
    out = f(id)
    print(f"({counter.fetch_inc()}/{total}) {name} {id} collected")
    return out
  return wrapped

pd.concat(
  tqdm_imap(
    pool=P,
    func=retry_exceptions(
      f=lambda id: commonteamroster.CommonTeamRoster(team_id=id, timeout=60, proxy=ips.get_proxy())\
        .get_data_frames()[0]
    ),
    iterable=set(leaguedf["TEAM_ID"]),
    desc="Updating players.csv"
  )
).reset_index(drop=True).to_csv("players.csv")

games = pd.concat(
  tqdm_imap(
    pool=P,
    func=retry_exceptions(
      f=lambda id: leaguegamefinder.LeagueGameFinder(team_id_nullable=id, timeout=60, proxy=ips.get_proxy())\
        .get_data_frames()[0],
    ),
    iterable=set(leaguedf["TEAM_ID"]),
    desc="Collecting games"
  )
).reset_index(drop=True)
games["GAME_ID"] = pd.to_numeric(games["GAME_ID"])
games.to_csv("games_raw.csv", index=False)

ahids = pd.concat(
  tqdm_imap(
    pool=P,
    func=retry_exceptions(
      f=lambda id: boxscoresummaryv2.BoxScoreSummaryV2(game_id=str(id).zfill(10), timeout=60, proxy=ips.get_proxy())\
        .get_data_frames()[0],
    ),
    iterable=set(games["GAME_ID"]) - set(pd.read_csv("games.csv")["GAME_ID"]),
    desc="Collecting ahids"
  )
).reset_index(drop=True)

print("Ending ips background ip checking...")
ips.end_background_thread()

RECOL = ["TEAM_ID","TEAM_ABBREVIATION","TEAM_NAME","WL","MIN","PTS","FGM","FGA","FG_PCT","FG3M","FG3A","FG3_PCT","FTM","FTA","FT_PCT","OREB","DREB","REB","AST","STL","BLK","TOV","PF","PLUS_MINUS"]
DROP = ["SEASON_ID", "GAME_DATE", "MATCHUP"]

def get_home_games(out):
  print("generating home_games...")
  e = time.perf_counter()
  out["home_games"] = games[[
    (
      ahids[["GAME_ID", "HOME_TEAM_ID"]].values == ids
    ).all(axis=1).any() 
    for ids in games[["GAME_ID", "TEAM_ID"]].values
  ]].rename(columns={col: col + "_home" for col in RECOL}).drop(columns=DROP)
  print(f"generated home_games -\t{time.perf_counter() - e}s")

def get_away_games(out):
  print("generating away_games...")
  e = time.perf_counter()
  out["away_games"] = games[[
    (
      ahids[["GAME_ID", "VISITOR_TEAM_ID"]].values == ids
    ).all(axis=1).any() 
    for ids in games[["GAME_ID", "TEAM_ID"]].values
  ]].rename(columns={col: col + "_away" for col in RECOL}).drop(columns=DROP)
  print(f"generated away_games -\t{time.perf_counter() - e}s")

games_dict = {}
game_threads = [Thread(target=f, args=[games_dict]) for f in [get_away_games, get_home_games]]
[t.start() for t in game_threads]
[t.join() for t in game_threads]

print("joining home and away games...")
epoch = time.perf_counter()
games = ahids.join(
  games_dict["home_games"]\
    .join(
      games_dict["away_games"]\
        .set_index("GAME_ID"), 
      on="GAME_ID"
    )\
    .set_index("GAME_ID"),
  on="GAME_ID"
)
print(f"joined home and away games -\t{time.perf_counter() - epoch}s")

print("games.csv updating...")
epoch = time.perf_counter()
games.to_csv("games.csv", index=False, mode="a")
print(f"games.csv updated -\t{time.perf_counter() - epoch}s")

print(f"all data updated in {time.perf_counter() - GENISIS} seconds")