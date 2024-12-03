# Copyright 2024 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import numpy as np

from astropy.convolution import Box2DKernel, Gaussian2DKernel
from astropy.convolution import convolve as astropy_convolve
from astropy.visualization import AsymmetricPercentileInterval, simple_norm

from line_profiler import profile


def sigmoid(img: list) -> list:
    """Sigmoid function used for img_normalizer

    Parameters
    ----------
    img: float array
        a float array representing a non-normalized image

    Returns
    -------
    out: float array
    """
    # Compute mean and std of the image
    img_mean, img_std = img.mean(), img.std()
    # restore img to normal mean and std
    img_normalize = (img - img_mean) / img_std
    # image inversion
    inv_norm = -img_normalize
    # compute exponential of inv img
    exp_norm = np.exp(inv_norm)
    # perform sigmoid calculation and return it
    return 1 / (1 + exp_norm)


def sigmoid_normalizer(img: list, vmin: float, vmax: float) -> list:
    """Image normalisation between vmin and vmax using Sigmoid function

    Parameters
    ----------
    img: float array
        a float array representing a non-normalized image

    Returns
    -------
    out: float array where data are bounded between vmin and vmax
    """
    return (vmax - vmin) * sigmoid(img) + vmin


def legacy_normalizer(data: list, stretch="asinh", pmin=0.5, pmax=99.5) -> list:
    """Old cutout normalizer which use the central pixel

    Parameters
    ----------
    data: float array
        a float array representing a non-normalized image

    Returns
    -------
    out: float array where data are bouded between vmin and vmax
    """
    size = len(data)
    vmax = data[int(size / 2), int(size / 2)]
    vmin = np.min(data) + 0.2 * np.median(np.abs(data - np.median(data)))
    return _data_stretch(
        data, vmin=vmin, vmax=vmax, pmin=pmin, pmax=pmax, stretch=stretch
    )


@profile
def convolve(image, smooth=3, kernel="gauss"):
    """Convolve 2D image. Hacked from aplpy"""
    if smooth is None and isinstance(kernel, str) and kernel in ["box", "gauss"]:
        return image

    if smooth is not None and not np.isscalar(smooth):
        raise ValueError(
            "smooth= should be an integer - for more complex "
            "kernels, pass an array containing the kernel "
            "to the kernel= option"
        )

    # The Astropy convolution doesn't treat +/-Inf values correctly yet, so we
    # convert to NaN here.
    image_fixed = np.array(image, dtype=float, copy=True)
    image_fixed[np.isinf(image)] = np.nan

    if isinstance(kernel, str):
        if kernel == "gauss":
            kernel = Gaussian2DKernel(smooth, x_size=smooth * 5, y_size=smooth * 5)
        elif kernel == "box":
            kernel = Box2DKernel(smooth, x_size=smooth * 5, y_size=smooth * 5)
        else:
            raise ValueError(f"Unknown kernel: {kernel}")

    return astropy_convolve(image, kernel, boundary="extend")


@profile
def _data_stretch(
    image,
    vmin=None,
    vmax=None,
    pmin=0.25,
    pmax=99.75,
    stretch="linear",
    vmid: float = 10,
    exponent=2,
):
    """Hacked from aplpy"""
    if vmin is None or vmax is None:
        interval = AsymmetricPercentileInterval(pmin, pmax, n_samples=10000)
        try:
            vmin_auto, vmax_auto = interval.get_limits(image)
        except IndexError:  # no valid values
            vmin_auto = vmax_auto = 0

    if vmin is None:
        # log.info("vmin = %10.3e (auto)" % vmin_auto)
        vmin = vmin_auto
    else:
        pass
        # log.info("vmin = %10.3e" % vmin)

    if vmax is None:
        # log.info("vmax = %10.3e (auto)" % vmax_auto)
        vmax = vmax_auto
    else:
        pass
        # log.info("vmax = %10.3e" % vmax)

    if stretch == "arcsinh":
        stretch = "asinh"

    normalizer = simple_norm(
        image,
        stretch=stretch,
        power=exponent,
        asinh_a=vmid,
        min_cut=vmin,
        max_cut=vmax,
        clip=False,
    )

    data = normalizer(image, clip=True).filled(0)
    data = np.nan_to_num(data)
    # data = np.clip(data * 255., 0., 255.)

    return data  # .astype(np.uint8)
