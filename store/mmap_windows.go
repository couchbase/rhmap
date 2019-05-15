//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// +build windows

package store

// Windows MapViewOfFile() API (rough equivalent of mmap()), requires
// region offsets to be multiples of an "allocation granularity",
// which is up to 64kiB (or, larger than the usual 4KB page size).
//
// See: https://social.msdn.microsoft.com/Forums/vstudio/en-US/972f36a4-26c9-466b-861a-5f40fa4cf4e7/about-the-dwallocationgranularity?forum=vclanguage
//
var MMapPageGranularity = 65536 // 64kiB.
