import pandas as pd


def TransformData(**kwargs):
    ti = kwargs['ti']
    dados = ti.xcom_pull(task_ids='extract_data')
    df = pd.json_normalize(dados)

    df_area = df[['area.id', 'area.name', 'area.code']].drop_duplicates().rename(
        columns={'area.id': 'id', 'area.name': 'name', 'area.code': 'code'}
    )

    df_winners = df[['currentSeason.winner.id', 'currentSeason.winner.name', 'currentSeason.winner.shortName',
                 'currentSeason.winner.tla',
                 'currentSeason.winner.crest', 'currentSeason.winner.address', 'currentSeason.winner.website',
                 'currentSeason.winner.founded', 'currentSeason.winner.clubColors', 'currentSeason.winner.venue',
                 'currentSeason.winner.lastUpdated']].dropna(
        subset=['currentSeason.winner.id']).drop_duplicates().rename(
        columns=lambda x: x.replace('currentSeason.winner.', '')
    )

    df_seasons = df[
        ['currentSeason.id', 'currentSeason.startDate', 'currentSeason.endDate', 'currentSeason.currentMatchday',
        'currentSeason.winner.id']].rename(
        columns={
            'currentSeason.id': 'id',
            'currentSeason.startDate': 'start_date',
            'currentSeason.endDate': 'end_date',
            'currentSeason.currentMatchday': 'current_matchday',
            'currentSeason.winner.id': 'winner_id'
        }
    ).drop_duplicates()

    df_competitions = df.drop(columns=[
        'area.id', 'area.name', 'area.code',
        'currentSeason.id', 'currentSeason.startDate', 'currentSeason.endDate', 'currentSeason.currentMatchday',
        'currentSeason.winner.id',
        'currentSeason.winner.name', 'currentSeason.winner.shortName', 'currentSeason.winner.tla',
        'currentSeason.winner.crest',
        'currentSeason.winner.address', 'currentSeason.winner.website', 'currentSeason.winner.founded',
        'currentSeason.winner.clubColors',
        'currentSeason.winner.venue', 'currentSeason.winner.lastUpdated'
    ])

    df_competitions['area_id'] = df['area.id']
    df_competitions['current_season_id'] = df['currentSeason.id']

    return [df_area, df_winners, df_seasons, df_competitions]
