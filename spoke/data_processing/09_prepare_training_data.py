import pandas as pd

INPUT_FILE_DATASET = "unified_dataset.parquet"

OUTPUT_FILE_TRAIN = "train.parquet"
OUTPUT_FILE_EVAL = "eval.parquet"
OUTPUT_FILE_TEST = "test.parquet"

def train_validation_test_split(
    *arrays, train_ratio=0.6, validation_ratio=0.2, test_ratio=0.2, **kwargs
):
    assert(train_ratio + validation_ratio + test_ratio == 1.0)
    from sklearn.model_selection import train_test_split
    from functools import reduce

    # Generate the training split and leave everything else for further splitting
    train_rest = train_test_split(
        *arrays,
        **kwargs,
        test_size=1 - train_ratio,
    )

    # Generate the validation and test splits from the remaining data
    val_test = train_test_split(
        # Select the "rest" elements from the list of tuples.
        *[train_rest[i + 1] for i in range(0, len(train_rest), 2)],
        **kwargs,
        shuffle=False,
        test_size=test_ratio / (test_ratio + validation_ratio),
    )

    return reduce(
        list.__add__,
        (
            # Combine the training sets from the first split with the
            # validation and test sets from the second split
            [train_rest[i], val_test[i], val_test[i + 1]]
            for i in range(0, len(train_rest), 2)
        ),
    )


def process(random_state, train_ratio, validation_ratio, test_ratio):
    unified_df = pd.read_parquet(INPUT_FILE_DATASET)

    X = unified_df.drop(
        columns=[
            # Columns that should not be relevant or would be falsely-relevant
            "NODE_ID",
            "NODE_LATITUDE",
            "NODE_LONGITUDE",
            "EVENT_DIST_FROM_NODE",
            # Columns that cannot logically be used to infer a crash
            "NUMBER OF PERSONS INJURED",
            "NUMBER OF PERSONS KILLED",
            "NUMBER OF PEDESTRIANS INJURED",
            "NUMBER OF PEDESTRIANS KILLED",
            "NUMBER OF CYCLIST INJURED",
            "NUMBER OF CYCLIST KILLED",
            "NUMBER OF MOTORIST INJURED",
            "NUMBER OF MOTORIST KILLED",
        ]
    )

    event_dt = pd.to_datetime(
        X.EVENT_DATE + " " + X.EVENT_TIME, infer_datetime_format=True
    )

    # Convert EVENT_TIME to be the number of seconds since midnight of that day that the crash occurred.
    seconds_since_midnight = (event_dt - event_dt.dt.normalize()).dt.total_seconds()
    X.EVENT_TIME = seconds_since_midnight

    # Convert EVENT_DATE to be the day of the year
    X.EVENT_DATE = event_dt.dt.day_of_year

    return train_validation_test_split(
        X,
        random_state=random_state,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        test_ratio=test_ratio,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("random_state", type=int)
    parser.add_argument("train_ratio", type=float)
    parser.add_argument("validation_ratio", type=float)
    parser.add_argument("test_ratio", type=float)
    args = parser.parse_args()
    X_train, X_eval, X_test = process(
        args.random_state, args.train_ratio, args.validation_ratio, args.test_ratio
    )
    X_train.to_parquet(OUTPUT_FILE_TRAIN)
    X_eval.to_parquet(OUTPUT_FILE_EVAL)
    X_test.to_parquet(OUTPUT_FILE_TEST)
