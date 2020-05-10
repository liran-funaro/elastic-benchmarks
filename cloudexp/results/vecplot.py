"""
Author: Liran Funaro <liran.funaro@gmail.com>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import numpy as np
from matplotlib import pylab as plt


def visualize_2d_vector(vec: np.array, d_vals=None, d_keys=None, d_limits=None, val_key=None, val_limits=None,
                        d_ticks=None, annotate=True, ticks_format=None, fmt='0.2f', mask_zero=False, mask=None,
                        **kwargs):
    if d_vals is None:
        d_vals = np.arange(vec.shape[0]), np.arange(vec.shape[1])

    if val_limits is not None:
        kwargs['vmin'] = val_limits[0]
        kwargs['vmax'] = val_limits[1]

    if mask is None and mask_zero:
        mask = np.isclose(vec, 0)

    if annotate:
        d_vals = np.arange(vec.shape[0]) + 0.5, np.arange(vec.shape[1]) + 0.5
        if mask is not None:
            mask = np.transpose(mask)
        import seaborn as sns
        sns.heatmap(np.transpose(vec), annot=annotate, cmap='coolwarm', fmt=fmt, linewidths=.05, mask=mask,
                    cbar_kws={'label': val_key},
                    **kwargs)
    else:
        cmap = plt.cm.coolwarm
        if mask is not None:
            cmap.set_bad(color='white')
            vec = np.ma.masked_where(mask, vec)
        plt.pcolor(d_vals[0], d_vals[1], np.transpose(vec), cmap=cmap, **kwargs)

    if d_keys is not None:
        plt.xlabel(d_keys[0])
        plt.ylabel(d_keys[1])
    if d_limits is not None:
        plt.xlim(d_limits[0])
        plt.ylim(d_limits[1])
    if d_ticks is not None:
        if ticks_format is None:
            ticks_format = lambda d: d
        plt.xticks(d_vals[0], map(ticks_format, d_ticks[0]), rotation='vertical')
        plt.yticks(d_vals[1], map(ticks_format, d_ticks[1]), rotation='horizontal')
    if not annotate:
        cbar = plt.colorbar()
        if val_key is not None:
            cbar.set_label(val_key, rotation=270)


def visualize_2d_vector_wire(vec: np.array, d_vals=None, d_keys=None, d_limits=None, val_key=None, val_limits=None,
                             d_ticks=None, view_init=None, **kwargs):
    # noinspection PyUnresolvedReferences
    from mpl_toolkits.mplot3d import Axes3D
    ax = plt.gca(projection='3d')
    ax.patch.set_facecolor('white')
    ax.w_xaxis.set_pane_color((1, 1, 1, 1.0))
    ax.w_yaxis.set_pane_color((1, 1, 1, 1.0))
    ax.w_zaxis.set_pane_color((1, 1, 1, 1.0))

    if d_vals is None:
        d_vals = np.arange(vec.shape[0]), np.arange(vec.shape[1]),

    sample_mesh = np.meshgrid(np.arange(vec.shape[0]), np.arange(vec.shape[1]), indexing='ij')

    x = d_vals[0]
    y = d_vals[1]
    x_mesh, y_mesh = np.meshgrid(x, y, indexing='ij')
    z_mesh = vec[tuple(sample_mesh)]
    ax.plot_wireframe(x_mesh, y_mesh, z_mesh, antialiased=False,  **kwargs)

    if d_keys is not None:
        ax.set_xlabel(d_keys[0])
        ax.set_ylabel(d_keys[1])
    if d_limits is not None:
        ax.set_xlim(d_limits[0])
        ax.set_ylim(d_limits[1])
    if d_ticks is not None:
        xx = ax.get_xticks()
        ax.set_xticklabels(np.interp(xx, x, d_ticks[0]))
        yy = ax.get_yticks()
        ax.set_yticklabels(np.interp(yy, y, d_ticks[1]))
    if val_key is not None:
        ax.set_zlabel(val_key)
    if val_limits is not None:
        ax.set_zlim(val_limits)

    if view_init is not None:
        ax.view_init(view_init)
    return ax


def visualize_2d_vector_multiline(vec: np.array, d_vals=None, d_keys=None, d_limits=None, val_key=None,
                                  val_limits=None, d_ticks=None, view_init=None, **kwargs):
    if vec.ndim != 2:
        raise ValueError("Vector dim must be 2.")

    if d_vals is None:
        d_vals = np.arange(vec.shape[0]), np.arange(vec.shape[1])

    x_dim = 0
    line_dim = 1
    lines_index = np.arange(vec.shape[line_dim])
    x = np.arange(vec.shape[x_dim])

    ax = plt.gca()
    for i in lines_index:
        ax.plot(x, vec[:, i], label=f'{d_ticks[line_dim][i]}')

    if d_keys is not None:
        ax.set_xlabel(d_keys[x_dim])
    # if d_limits is not None:
    #     ax.set_xlim(d_limits[0])
    #     ax.set_ylim(d_limits[1])
    if d_ticks is not None:
        xx = ax.get_xticks()
        ax.set_xticklabels(np.interp(xx, x, d_ticks[x_dim]))
    if val_key is not None:
        ax.set_ylabel(val_key)
    # if val_limits is not None:
    #     ax.set_zlim(val_limits)
    plt.legend(title=d_keys[line_dim])
    return ax
