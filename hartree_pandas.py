# Using Python 3.11.1 and pandas 1.5.3

import itertools

import pandas as pd

df1 = pd.read_csv('dataset1.csv')
df2 = pd.read_csv('dataset2.csv')

# Join the two dataframes on counter_party
df = pd.merge(df1, df2, on='counter_party')

# Generate all combinations of legal_entity, counter_party and tier with Total
legal_entity = df['legal_entity'].unique().tolist()
legal_entity.append('Total')
counter_party = df['counter_party'].unique().tolist()
counter_party.append('Total')
tier = df['tier'].unique().tolist()
tier.append('Total')

# Use the itertool product function to create all the combinations
# of legal_entity, counter_party and tier
combinations = itertools.product(legal_entity, counter_party, tier)

output_rows = []
for row in combinations:
    # From the example output I assume that only rows for which on of the columns
    # gets aggregated are of interest
    if 'Total' not in row:
        continue

    # Filter the dataframe based on the legal_entity, counter_party and tier
    # combination of this iteration
    df_filtered = df.copy()
    cols = {'legal_entity': row[0], 'counter_party': row[1], 'tier': row[2]}
    for col, value in cols.items():
        if value == 'Total':
            continue
        df_filtered = df_filtered.loc[df[col] == value]

    # Skip if the combination does not show up in the datasets
    # L1, C2, Total for example
    if len(df_filtered) == 0:
        continue

    # Calculate the aggreagated values
    max_rating_by_counterparty = (
        df_filtered.groupby(['counter_party'])['rating'].max().to_dict()
    )
    sum_value_status_ARAP = df_filtered[df_filtered['status'] == 'ARAP']['value'].sum()
    sum_value_status_ACCR = df_filtered[df_filtered['status'] == 'ACCR']['value'].sum()

    # Add the aggregated columns to the output rows
    output_rows.append(
        {
            'legal_entity': row[0],
            'counter_party': row[1],
            'tier': row[2],
            'max(rating by counterparty)': max_rating_by_counterparty,
            'sum(value where status=ARAP)': sum_value_status_ARAP,
            'sum(value where status=ACCR)': sum_value_status_ACCR,
        }
    )

df_out = pd.DataFrame(output_rows)
df_out.to_csv('output_pandas.csv', index=False)
