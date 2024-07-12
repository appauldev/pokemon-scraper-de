from dataclasses import dataclass, field
import pandas as pd
import duckdb
from bs4 import BeautifulSoup, Tag
from dagster import asset
import httpx
import os
from . import constants
from . import urls


@dataclass
class Pokemon:
    number: str
    name: str
    pktypes: str
    total: int
    hp: int
    attack: int
    defense: int
    sp_atk: int
    sp_def: int
    speed: int


@asset
def scraped_html_pokemon_data_all():
    # prep file paths
    POKEMON_ALL_HTML = os.path.join(
        os.path.dirname(__file__), constants.HTML_POKEMON_ALL
    )

    # fetch the data
    print("fetching pokemon data")
    response = httpx.get(urls.POKEMON_ALL)
    print("data fetched!")

    # save the fetched data
    with open(POKEMON_ALL_HTML, "+wb") as f:
        f.write(response.content)
        print("done")


@asset(deps=[scraped_html_pokemon_data_all])
def tbl_pokemon_data_all():
    HTML_POKEMON_ALL_FILE_PATH = os.path.join(
        os.path.dirname(__file__), constants.HTML_POKEMON_ALL
    )

    with open(HTML_POKEMON_ALL_FILE_PATH) as f:
        soup = BeautifulSoup(f)

    pokemon_table = soup.find("tbody")
    pokemon_rows: list[Tag] = pokemon_table.find_all("tr")
    print(f"count of pokemon: {len(pokemon_rows)}")
    # print(f"{pokemon_rows=}")

    # extract pokemon attributes
    pokemon_attributes: list[list[Tag]] = []
    for tag in pokemon_rows:
        attributes: list[Tag] = tag.find_all("td")
        pokemon_attributes.append(attributes)

    pokemons: list[Pokemon] = []
    for attr in pokemon_attributes:
        pktypes = []
        for type in attr[2].find_all("a"):
            pktypes.append(type.string)

        pokemon = Pokemon(
            number=attr[0].find("span").string,
            name=attr[1].find("a").string,
            pktypes=",".join(pktypes),
            total=int(attr[3].string),
            hp=int(attr[4].string),
            attack=int(attr[5].string),
            defense=int(attr[6].string),
            sp_atk=int(attr[7].string),
            sp_def=int(attr[8].string),
            speed=int(attr[9].string),
        )
        # print(pokemon)
        pokemons.append(pokemon)

    df = pd.DataFrame(pokemons)
    CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), "datasets/pokemon.csv")
    df.to_csv(CSV_FILE_PATH, mode="w", index=False)
    # print(df)
    # print(df.dtypes)

    DUCKDB_POKEMON_FILE_PATH = os.path.join(
        os.path.dirname(__file__), constants.DUCKDB_POKEMON
    )
    with duckdb.connect(DUCKDB_POKEMON_FILE_PATH) as conn:
        query_create_tbl = f"""
            CREATE OR REPLACE SEQUENCE pk_seq START 1;

            CREATE OR REPLACE TABLE pokemons (
                id BIGINT DEFAULT NEXTVAL('pk_seq'),
                number TEXT,
                name TEXT,
                pktypes TEXT,
                total SMALLINT,
                hp SMALLINT,
                attack SMALLINT,
                defense SMALLINT,
                sp_atk SMALLINT,
                sp_def SMALLINT,
                speed SMALLINT
            );

            INSERT INTO pokemons (
                number,
                name,
                pktypes,
                total,
                hp,
                attack,
                defense,
                sp_atk,
                sp_def,
                speed
            )
                SELECT * FROM read_csv('{CSV_FILE_PATH}');
        """
        conn.sql(query_create_tbl)
