import sys
import numpy as np
sys.path.append('/home/papec/Work/my_projects/z5/bld/python')


def test_output():
    import z5py
    data = z5py.File('./output.n5')['out'][:]
    print(np.sum(data), '/', data.size)
    assert np.allclose(data, 1), "Failed !"
    print("Passed !")


if __name__ == '__main__':
    test_output()
