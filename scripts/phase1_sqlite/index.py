import sqlite3

with sqlite3.connect("../../data/imdb.db") as conn:
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_mt_primary
        ON movie_titles(is_primary);
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pr_movie_id
        ON principals(movie_id);
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pr_person_profession_movie
        ON principals(person_id, profession_id, movie_id);
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_m_year
        ON movies(year);
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pe_name
        ON persons(name);
    """)
