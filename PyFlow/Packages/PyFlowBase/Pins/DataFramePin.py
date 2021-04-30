## Copyright 2015-2019 Ilgar Lunin, Pedro Cabrera

## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at

##     http://www.apache.org/licenses/LICENSE-2.0

## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

import pandas as pd
from PyFlow.Core import PinBase
from PyFlow.Core.Common import *
from PyFlow.DataFrameManager import DataFrameManager

class DataFramePin(PinBase):
    """doc string for DataFramePin"""
    def __init__(self, name, parent, direction, **kwargs):
        super(DataFramePin, self).__init__(name, parent, direction, **kwargs)
        self.setDefaultValue(pd.DataFrame())
        self.data_frame_manager = DataFrameManager()

    @staticmethod
    def IsValuePin():
        return True

    @staticmethod
    def supportedDataTypes():
        return ('DataFramePin', )

    @staticmethod
    def pinDataTypeHint():
        return 'DataFramePin', False

    @staticmethod
    def color():
        return (255, 0, 0, 255)

    @staticmethod
    def internalDataStructure():
        return pd.DataFrame

    @staticmethod
    def processData(data):
        return DataFramePin.internalDataStructure()(data)