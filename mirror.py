#!python
from pathlib import Path
from pyquery import PyQuery
from typing import Any
from typing import Generator
from urllib.error import HTTPError
from urllib.parse import parse_qs
from urllib.parse import urlparse
from urllib.request import urlopen
import json
import re


BASE_PATH = Path(__file__).absolute().parent / 'data'
BASE_URL = 'https://www.doom.com.hr/'
UNKNOWN = 'unknown'


def fetch(path: Path, url: str) -> None:
    tmp_path = path.with_suffix('.tmp')
    with tmp_path.open('wb') as f:
        try:
            with urlopen(url) as r:
                f.write(r.read())
        except HTTPError as e:
            print()
            print(e)
            tmp_path.unlink()
            return
    tmp_path.rename(path)


def fetchdb(base_path: Path, base_url: str, key: str, value: int | None) -> str:
    url_args = ''
    if value is not None:
        url_args = f'{key}={value}'
    path = base_path / ('root.html' if value is None else f'{value}.html')
    if not path.exists():
        url = f'{base_url}&{url_args}' if url_args else base_url
        fetch(path, url)
    with path.open() as f:
        return f.read()


def get_qs(elem: Any) -> dict:
    parsed_url = urlparse(elem.attrib['href'])
    return parse_qs(parsed_url.query)


def get_id(elem: Any, name: str) -> int:
    (value,) = get_qs(elem)[name]
    return int(value)


def update_ids(pq: PyQuery, ids: dict, name: str) -> None:
    for link in pq('p a'):
        if link.text == 'all':
            continue
        qs = get_qs(link)
        if name != 'cndb':
            qs.pop('cndb', None)
        qs.pop('page', None)
        keys = set(qs.keys())
        if name in keys and not keys.difference({name}):
            (value,) = qs[name]
            value = int(value)
            ids.setdefault(value, {}).setdefault('short', link.text)
            assert ids[value]['short'] == link.text, (
                f'{ids[value]["short"]=} != {link.text=}')


def update_players(base_path: Path, base_url: str, ids: dict) -> None:
    if not base_path.exists():
        base_path.mkdir()
    players = ids.setdefault('players', {})
    pq = PyQuery(fetchdb(base_path, base_url, '', None))
    for elem in pq('.post li'):
        parts = elem.text_content().partition('(ID:')
        assert parts[1] == '(ID:', parts
        player_id = int(parts[2].strip(')'))
        if m := re.match(r".+(\s'(.+)')\s.+\s*", parts[0]):
            name = parts[0][:m.span(1)[0]] + parts[0][m.span(1)[1]:]
            nick = m[2]
        else:
            name = parts[0]
            nick = None
        name = name.strip()
        if player_id not in players:
            players.setdefault(player_id, {}).setdefault('name', name)
            if nick is not None:
                players.setdefault(player_id, {}).setdefault('nick', nick)
        assert players[player_id]['name'] == name, (
            f'{players[player_id]["name"]} != {name}')
        assert players[player_id].get('nick') == nick, (
            f'{players[player_id].get("nick")} != {nick}')
        if links := elem.cssselect('a'):
            (link,) = links
            assert player_id == get_id(link, 'player_id')
            fetchdb(base_path, base_url, 'player_id', player_id)


def update_root(base_path: Path, base_url: str, ids: dict) -> None:
    pq = PyQuery(fetchdb(base_path, base_url, '', None))
    cndbs = ids.setdefault('cndbs', {})
    update_ids(pq, cndbs, 'cndb')


def update_cndbs(
        base_path: Path, db_path: Path, base_url: str, orig_ids: dict) -> None:
    cndb_path = db_path / 'cndb'
    if not cndb_path.exists():
        cndb_path.mkdir()
    for cndb in orig_ids['cndbs']:
        print("\n", orig_ids['cndbs'][cndb])
        db_base_url = f'{base_url}&cndb={cndb}'
        ids: dict = {}
        pq = PyQuery(fetchdb(cndb_path, base_url, 'cndb', cndb))
        categories = ids.setdefault('categories', {})
        cndbs = ids.setdefault('cndbs', {})
        wads = ids.setdefault('wads', {})
        update_ids(pq, categories, 'category_id')
        update_ids(pq, cndbs, 'cndb')
        update_ids(pq, wads, 'wad_id')
        todo = set(update_wads(cndb_path / f'{cndb}-wads', db_base_url, ids))
        count = 0
        total = len(todo)
        while todo:
            count += 1
            (kind, key) = todo.pop()
            print(f'\r{count}/{total}', end='')
            print(f' {kind} {key:<20}', end='')
            match kind:
                case 'map':
                    new = set(update_map(
                        cndb_path / f'{cndb}-maps', base_url, db_base_url, ids, key))
                case 'player':
                    new = set(update_player(
                        cndb_path / f'{cndb}-players', base_url, db_base_url, ids, key))
                case _:
                    raise RuntimeError(f'{(kind, key)=!r}')
            total += len(new)
            todo.update(new)
        print()
        print("Players:", len(ids['players']))
        print("Records:", len(ids['records']), min(ids['records']), max(ids['records']))
        update_records(base_path, base_url, ids)
        update_demos(base_path, ids['records'])


def update_wads(wads_path: Path, base_url: str, ids: dict) -> Generator:
    if not wads_path.exists():
        wads_path.mkdir()
    wads = ids['wads']
    for index, wad_id in enumerate(wads, 1):
        print(f'\rWAD: {index}/{len(wads)}', end='')
        pq = PyQuery(fetchdb(wads_path, base_url, 'wad_id', wad_id))
        yield from update_from_table(base_url, pq, ids)
    print()


def update_from_table(  # noqa: PLR0915
        base_url: str, pq: PyQuery, ids: dict) -> Generator:
    categories = ids['categories']
    maps = ids.setdefault('maps', {})
    players = ids.setdefault('players', {})
    records = ids.setdefault('records', {})
    wads = ids['wads']
    headers: list
    (category_title, wad_title, headers) = (None, None, [])
    for tr in pq('table.competn_records_table tr'):
        tags = {x.tag for x in tr}
        if len(tr) == 1 and tags == {'th'}:
            parts = tr.text_content().partition('-')
            if parts[1] == '-':
                wad_title = parts[0].strip()
                category_title = parts[2].strip()
        elif tags == {'th'}:
            assert headers == []
            headers = [x.text_content().lower() for x in tr]
        elif tr[0].tag != 'td':
            continue
        else:
            cells = dict(zip(
                headers,
                (
                    l[0] if len(l := x.cssselect('a')) == 1 else x.text_content()
                    for x in tr),
                strict=True))
            record_link = cells['filename']
            record_link.make_links_absolute(base_url)
            record_id = get_id(record_link, 'record_id')
            record = records.setdefault(record_id, {})
            record.setdefault('filename', record_link.text.strip())
            assert record['filename'] == record_link.text.strip(), (
                f'{record["filename"]=} != {record_link.text.strip()=}')
            href = record_link.attrib['href'].replace('cndb=1&', 'cndb=&')
            record.setdefault('href', href)
            assert record['href'] == href, (
                f'{record["href"]=} != {href=}')
            record.setdefault('date', cells['date'])
            assert record['date'] == cells['date'], (
                f'{record["date"]=} != {cells["date"]=}')
            record.setdefault('time', cells['time'])
            assert record['time'] == cells['time'], (
                f'{record["time"]=} != {cells["time"]=}')
            map_link = cells['map']
            map_id = get_id(map_link, 'map_id')
            record.setdefault('map_id', map_id)
            assert record['map_id'] == map_id, (
                f'{record["map_id"]=} != {map_id=}')
            name = map_link.text.strip()
            if map_id not in maps:
                maps.setdefault(map_id, name)
                yield ('map', map_id)
            assert maps[map_id] == name, f'{maps[map_id]=} != {name=}'
            category_id = get_id(map_link, 'category_id')
            record.setdefault('category_id', category_id)
            assert record['category_id'] == category_id, (
                f'{record["category_id"]=} != {category_id=}')
            if category_title is not None:
                categories[category_id].setdefault('title', category_title)
                assert categories[category_id]['title'] == category_title, (
                    f'{categories[category_id]["title"]=} != {category_title=}')
                category_title = None
            wad_id = get_id(map_link, 'wad_id')
            record.setdefault('wad_id', wad_id)
            assert record['wad_id'] == wad_id, (
                f'{record["wad_id"]=} != {wad_id=}')
            if wad_title is not None:
                wads[wad_id].setdefault('title', wad_title)
                assert wads[wad_id]['title'] == wad_title, (
                    f'{wads[wad_id]["title"]=} != {wad_title=}')
                wad_title = None
            player_link = cells['player']
            player_id = get_id(player_link, 'player_id')
            record.setdefault('player_id', player_id)
            assert record['player_id'] == player_id, (
                f'{record["player_id"]=} != {player_id=}')
            name = player_link.text.strip()
            if player_id not in players:
                players.setdefault(player_id, {}).setdefault('name', name)
                yield ('player', player_id)
            assert players[player_id]['name'] == name, (
                f'{players[player_id]["name"]} != {name}')


def update_map(
        base_path: Path,
        base_url: str, db_base_url: str,
        ids: dict, map_id: int) -> Generator:
    if not base_path.exists():
        base_path.mkdir()
    pq = PyQuery(fetchdb(base_path, db_base_url, 'map_id', map_id))
    yield from update_from_table(base_url, pq, ids)


def update_player(
        base_path: Path,
        base_url: str, db_base_url: str,
        ids: dict, player_id: int) -> Generator:
    if not base_path.exists():
        base_path.mkdir()
    pq = PyQuery(fetchdb(base_path, db_base_url, 'player_id', player_id))
    yield from update_from_table(base_url, pq, ids)


def update_records(base_path: Path, base_url: str, ids: dict) -> None:
    categories = ids['categories']
    maps = ids['maps']
    players = ids['players']
    records = ids['records']
    wads = ids['wads']
    for index, record_id in enumerate(records, 1):
        record = records.get(record_id)
        if record is None:
            fn = UNKNOWN
            href = f'{base_url}index.php?page=download&record_id={record_id}'
        else:
            fn = record['filename']
            href = record['href']
            data = dict(
                category=categories[record['category_id']],
                date=record['date'],
                map=maps[record['map_id']],
                player=players[record['player_id']],
                time=record['time'],
                wad=wads[record['wad_id']])
        print(f'\rRecord: {index}/{len(records)}', end='')
        print(f' {record_id} {fn:10}', end='')
        json_path = base_path / 'records' / f'{record_id}.json'
        if not json_path.exists():
            print(f' {json_path.name:10}', end='')
            with urlopen(href) as r:
                refresh = r.getheader('Refresh')
                if refresh is None:
                    continue
                parts = refresh.partition('=')
                assert parts[1] == '=', parts
                assert parts[0] == '1; url', parts
                url = parts[2]
            data['url'] = url
            if fn == UNKNOWN:
                parsed_path = Path(urlparse(url).path)
                fn = parsed_path.name
                if 'category' not in data:
                    data['category'] = dict(short=parsed_path.parent.name)
                if 'wad' not in data:
                    data['wad'] = dict(short=parsed_path.parent.parent.name)
            data['filename'] = fn
            tmp_path = json_path.with_suffix('.tmp')
            with tmp_path.open('w') as f:
                try:
                    json.dump(data, f, allow_nan=False, indent=4, sort_keys=True)
                except Exception:
                    tmp_path.unlink()
                    raise
            tmp_path.rename(json_path)
    print()


def update_demos(base_path: Path, records: dict) -> None:
    demos_path = base_path / 'demos'
    if not demos_path.exists():
        demos_path.mkdir()
    for index, record_id in enumerate(records, 1):
        json_path = base_path / 'records' / f'{record_id}.json'
        with json_path.open() as f:
            data = json.load(f)
        assert data['url'].startswith('http://www.doom.com.hr/public/'), data['url']
        fn = data['filename']
        print(f'\rDemo: {index}/{len(records)}', end='')
        print(f' {record_id} {fn:10}', end='')
        old_demo_path = demos_path / fn
        demo_path = demos_path / data['url'].removeprefix('http://www.doom.com.hr/public/')
        if not demo_path.exists():
            demo_path.parent.mkdir(parents=True, exist_ok=True)
            if old_demo_path.exists():
                old_demo_path.rename(demo_path)
            else:
                url = data['url'].replace('#', '%23')
                print(f' {url}', end='')
                fetch(demo_path, url)
        if not demo_path.exists():
            print(data)
    print()


def mirror(base_path: Path, base_url: str) -> None:
    ids: dict = {}
    update_players(
        base_path / 'players',
        f'{base_url}index.php?page=compet-n_players',
        ids)
    db_path = base_path / 'db'
    db_base_url = f'{base_url}index.php?page=compet-n_database'
    update_root(db_path, db_base_url, ids)
    update_cndbs(base_path, db_path, db_base_url, ids)


if __name__ == '__main__':
    mirror(BASE_PATH, BASE_URL)
