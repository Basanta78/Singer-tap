import os
import json
import singer
from singer import utils
from tap_student.ods import ODSConnection
import yaml

REQUIRED_CONFIG_KEYS = ["start_date"]
LOGGER = singer.get_logger()

KEY_PROPERTIES = {
    'students': ['id'],
}
config_file = open("./tap_student/config.yml")
config = yaml.load(config_file)
def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': [],
            'key_properties': KEY_PROPERTIES[schema_name]
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', True):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected', None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams


def get_all_promisestudents(schema, state):
    # api call to get all promise students...not functional
    provider = config["provider"]
    conn = ods.ODSConnection(provider, config["client_id"], config["client_secret"])
    students = conn.connect_domain('students')
    student_status_code, promisestudents = students.get_all()
    # request would call ODS API and return json
    extraction_time = utils.now()
    for student in promisestudents:
        with singer.Transformer() as transformer:
            rec = transformer.transform(student, schema)
        singer.write_record('students', rec, time_extracted=extraction_time)
        singer.write_bookmark(state, 'students', 'since', singer.utils.strftime(extraction_time))


SYNC_FUNCTIONS = {
    'students': get_all_promisestudents,
}


def sync(state, catalog):
    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        # if stream is selected, write schema and sync
        if stream_id in selected_stream_ids:
            singer.write_schema(stream_id, stream_schema, stream['key_properties'])

            # get sync function and any sub streams
            sync_func = SYNC_FUNCTIONS[stream_id]
            stream_schemas = {stream_id: stream_schema}
            state = sync_func(stream_schemas, state, )
            singer.write_state(state)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:

        # 'properties' is the legacy name of the catalog
        if args.properties:
            catalog = args.properties
        # 'catalog' is the current name
        elif args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(args.state, catalog)


if __name__ == "__main__":
    main()
