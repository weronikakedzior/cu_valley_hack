import csv
import sys
import numpy as np
from alibi_detect.cd import KSDrift

def get_ks_detector(reference_set, update_ref_set):
    if update_ref_set:
        return KSDrift(
            np.array(reference_set),
            p_val=p_val,
            update_x_ref={'last': reference_set_size}
        )
    else:
        return KSDrift(
            np.array(reference_set),
            p_val=p_val
        )

def predict(cd, val):
    prediction = cd.predict(
        np.array(val),
        drift_type='batch',
        return_p_val=True,
        return_distance=False
    )['data']
    return prediction['is_drift']

    with open(in_file_path, 'r') as f_in, open(out_file_path, 'w') as f_out:

        reader = csv.reader(f_in)
        # skip header
        next(reader)

        writer = csv.writer(f_out)
        # write header
        writer.writerow(['x', 'is_drift', 'p_value'])

        # prepare set to be reference in Kolmogorov-Smirnov test
        reference_set = []
        for i in range(reference_set_size):
            reference_set.append(float(next(reader)[0]))

        # method using Two-sample Kolmogorov–Smirnov test
        # to decide if testing sample comes from the same
        # distribution as reference set
        cd = _get_ks_detector(reference_set, update_reference_set)

        i = 0

        # save samples from reference set
        for val in reference_set:
            writer.writerow([val, 0, 0])

        # predcictions for the rest of samples
        test_set = []
        for val in reader:
            is_drift = 0
            p_val = 0
            val = float(val[0])
            test_set.append(val)

            if len(test_set) == test_set_size:
                detection = _predict(cd, test_set)
                is_drift = detection['is_drift']
                p_val = detection['p_val'][0]
                if verbose and is_drift:
                    _log_drift(val, i, p_val)
                test_set = []

            writer.writerow([val, is_drift, p_val])
            i += 1


if __name__ == '__main__':

    in_file_path = sys.argv[1]
    out_file_path = sys.argv[2]
    reference_set_size = int(sys.argv[3])
    update_reference_set = bool(int(sys.argv[4]))
    p_val = float(sys.argv[5])
    test_set_size = int(sys.argv[6])
    try:
        verbose = bool(int(sys.argv[7]))
    except KeyError:
        verbose = True

    detect(
        in_file_path=in_file_path,
        out_file_path=out_file_path,
        reference_set_size=reference_set_size,
        update_reference_set=update_reference_set,
        p_val=p_val,
        test_set_size=test_set_size,
        verbose=verbose
    )
