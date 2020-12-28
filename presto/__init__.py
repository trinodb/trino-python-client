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

__version__ = "0.303.0"

raise Exception("""
This was a package for PrestoSQL. The package itself is no longer maintained,
as PrestoSQL got renamed to Trino. Read more at
https://trino.io/blog/2020/12/27/announcing-trino.html

If you are using an older PrestoSQL release, you can install a previous
version of the package with:

    pip install presto-client==0.302.0
    
The package has been superseded with a client for Trino. You can install it
with:

    pip install trino
    
Apologies for the disruption and very short notice, resulting in no transition
period.
""")
