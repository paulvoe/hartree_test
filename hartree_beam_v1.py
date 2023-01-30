# Using Python 3.10.9 and Apache Beam 2.44.0

import apache_beam as beam


class ParseDataset1(beam.DoFn):
    def process(self, element):
        invoice_id, legal_entity, counter_party, rating, status, value = element.split(
            ','
        )
        return [
            {
                'invoice_id': int(invoice_id),
                'legal_entity': legal_entity,
                'counter_party': counter_party,
                'rating': int(rating),
                'status': status,
                'value': int(value),
            }
        ]


class ParseDataset2(beam.DoFn):
    def process(self, element):
        counter_party, tier = element.split(',')
        return [(counter_party, int(tier))]


class MergeDataset2(beam.DoFn):
    def process(self, element, d2):
        element['tier'] = d2[element['counter_party']]
        yield element


class ExpandWithCounterParties(beam.DoFn):
    def process(self, element, counter_parties):
        return [
            {
                'legal_entity': element,
                'counter_party': counter_party,
            }
            for counter_party in counter_parties
        ]


class ExpandWithTiers(beam.DoFn):
    def process(self, element, tiers):
        return [
            {
                'legal_entity': element['legal_entity'],
                'counter_party': element['counter_party'],
                'tier': tier,
            }
            for tier in tiers
            if element['legal_entity'] == 'Total'
            or element['counter_party'] == 'Total'
            or tier == 'Total'
        ]


def filter_combination(element, combination):
    return (
        (
            combination['legal_entity'] == 'Total'
            or element['legal_entity'] == combination['legal_entity']
        )
        and (
            combination['counter_party'] == 'Total'
            or element['counter_party'] == combination['counter_party']
        )
        and (combination['tier'] == 'Total' or element['tier'] == combination['tier'])
    )


def filter_ARAP(element):
    return element['value'] if element['status'] == 'ARAP' else 0


def filter_ACCR(element):
    return element['value'] if element['status'] == 'ACCR' else 0


class Aggregate(beam.DoFn):
    def process(self, combination, data):
        # This version uses Apache Beam PTransforms to filter and aggregate
        # the main dataset, that is passed as side input.
        # There seams to be some problem with using PTransforms on side input
        # PCollections, which makes this version very slow and emits a lot of
        # warnings about unparsable args for the PipelineOptions.
        # Either this is not the way to go, or I'm doing something wrong here.
        # I provide a pure python implementation of this Aggregate transform
        # as an alternative (hartree_beam_v2.py).
        filtered_data = data | beam.Filter(filter_combination, combination)
        count = filtered_data | 'Count elements' >> beam.combiners.Count.Globally()
        if count[0] > 0:
            sum_ARAP = filtered_data | beam.Map(filter_ARAP) | beam.CombineGlobally(sum)
            sum_ACCR = filtered_data | beam.Map(filter_ACCR) | beam.CombineGlobally(sum)
            max_ratings = filtered_data | beam.GroupBy(
                lambda e: e['counter_party']
            ).aggregate_field(lambda e: e['rating'], max, 'max_rating')

            yield {
                'legal_entity': combination['legal_entity'],
                'counter_party': combination['counter_party'],
                'tier': combination['tier'],
                'max(rating by counterparty)': max_ratings,
                'sum(value where status=ARAP)': sum_ARAP[0],
                'sum(value where status=ACCR)': sum_ACCR[0],
            }


class FormatOutput(beam.DoFn):
    def process(self, element):
        # Creates comma separated strings from the elements
        max_ratings = '{'
        for r in element['max(rating by counterparty)']:
            max_ratings += f'\'{r.key}\': {r.max_rating}, '
        max_ratings = max_ratings[:-2] + '}'
        if ',' in max_ratings:
            max_ratings = f'"{max_ratings}"'
        yield ','.join(
            [
                element['legal_entity'],
                element['counter_party'],
                str(element['tier']),
                max_ratings,
                str(element['sum(value where status=ARAP)']),
                str(element['sum(value where status=ACCR)']),
            ]
        )


with beam.Pipeline() as pipeline:
    # Load dataset2
    dataset2 = (
        pipeline
        | 'Load dataset2.csv'
        >> beam.io.ReadFromText('./dataset2.csv', skip_header_lines=1)
        | beam.ParDo(ParseDataset2())
    )

    # Load dataset1 and merge dataset2
    data = (
        pipeline
        | 'Load dataset1.csv'
        >> beam.io.ReadFromText('./dataset1.csv', skip_header_lines=1)
        | beam.ParDo(ParseDataset1())
        | beam.ParDo(MergeDataset2(), d2=beam.pvalue.AsDict(dataset2))
    )

    total_pcoll = pipeline | beam.Create(['Total'])
    # Find all distinct legal entities, counter parties and tiers
    legal_entites = (
        data
        | beam.Map(lambda e: e['legal_entity'])
        | 'Distinct legal_party' >> beam.Distinct()
    )
    legal_entites = (
        legal_entites,
        total_pcoll,
    ) | 'Append Total to legal_entites' >> beam.Flatten()

    counter_parties = (
        data
        | beam.Map(lambda e: e['counter_party'])
        | 'Distinct counter_party' >> beam.Distinct()
    )
    counter_parties = (
        counter_parties,
        total_pcoll,
    ) | 'Append Total to counter_parties' >> beam.Flatten()

    tiers = data | beam.Map(lambda e: e['tier']) | 'Distinct tier' >> beam.Distinct()
    tiers = (
        tiers,
        total_pcoll,
    ) | 'Append Total to tiers' >> beam.Flatten()

    # Compute all combinations of legal entities, counter parties and tiers
    # with Total
    combinations = (
        legal_entites
        | 'Expand with counter_parties'
        >> beam.ParDo(
            ExpandWithCounterParties(),
            counter_parties=beam.pvalue.AsIter(counter_parties),
        )
        | 'Expand with tiers'
        >> beam.ParDo(ExpandWithTiers(), tiers=beam.pvalue.AsIter(tiers))
    )

    # This pipeline creates a PCollection with the aggregated values for each
    # of the previously computed combinations
    # The resulting PCollection is then formatted as CSV and written to the output file
    output = (
        combinations
        | beam.ParDo(Aggregate(), data=beam.pvalue.AsIter(data))
        | beam.ParDo(FormatOutput())
        | beam.io.WriteToText(
            'output_beam_v1',
            header=(
                'legal_entity,counter_party,tier,max(rating by counterparty),'
                'sum(value where status=ARAP),sum(value where status=ACCR)'
            ),
            file_name_suffix='.csv',
        )
    )
    output | 'Print output filename' >> beam.Map(print)
