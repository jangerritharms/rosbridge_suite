# Software License Agreement (BSD License)
#
# Copyright (c) 2012, Willow Garage, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of Willow Garage, Inc. nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from rosbridge_library.capability import Capability
import math


class Fragmentation(Capability):
    """ The Fragmentation capability doesn't define any incoming operation
    handlers, but provides methods to fragment outgoing messages """

    fragmentation_seed = 0

    def __init__(self, protocol):
        # Call superclass constructor
        Capability.__init__(self, protocol)

    def fragment(self, message, fragment_size, mid=None):
        """ Serializes the provided message, then splits the serialized
        message according to fragment_size, then sends the fragments.

        If the size of the message is less than the fragment size, then
        the original message is returned rather than a single fragment

        Since fragmentation is typically only used for very large messages,
        this method returns a generator for fragments rather than a list

        Keyword Arguments
        message       -- the message dict object to be fragmented
        fragment_size -- the max size for the fragments
        mid           -- (optional) if provided, the fragment messages
        will be given this id.  Otherwise an id will be auto-generated.

        Returns a generator of message dict objects representing the fragments
        """
        # All fragmented messages need an ID so they can be reconstructed
        if mid is None:
            mid = self.fragmentation_seed
            self.fragmentation_seed = self.fragmentation_seed + 1

        serialized = self.protocol.serialize(message, mid)

        if serialized is None:
            return []

        message_length = len(serialized)
        if message_length <= fragment_size:
            return [message]

        msg_id = message.get("id", None)

        expected_duration = int(math.ceil(math.ceil(message_length / float(fragment_size))) * self.protocol.delay_between_messages)

        log_msg = "sending " + str(int(math.ceil(message_length / float(fragment_size)))) + " parts [fragment size: " + str(fragment_size) +"; expected duration: ~" + str(expected_duration) + "s]"
        self.protocol.log("info", log_msg)

        return self._fragment_generator(serialized, fragment_size, mid)

    def _fragment_generator(self, msg, size, mid):
        """ Returns a generator of fragment messages """
        msg = {
            "op": "fragment",
            "id": mid,
            "data": "",
            "num": 0,
            "total": 0,
            "fill": ""
        }
        header_size = len(str(msg))
        total = 0
        msg_length = 0
        try:
            (total, msg_length) = self._determine_n_fragments(len(msg), size, header_size)
        except RuntimeError:
            self.protocol.log("error", "Header size is to large for message size")
            return []
        msg['total'] = total
        n = 0
        for i in range(0, total):
            fragment = msg[i:i+msg_length]
            msg['data'] = fragment
            msg['num'] = n
            fill_length = size - len(str(fragment))
            msg['fill'] = "".join(["0" for i in xrange(fill_length)])
            n = n + 1

    def _determine_n_fragments(self, msg_len, size, header_size):
        """ Recursive function to determin the number and size of fragments """
        A = [msg_len + header_size]
        total = 1

        while A[-1]/total > size:
            msg_len += header_size
            temp = total +1
            temp_log = math.floor(math.log(temp)/math.log(10))
            total_log = math.floor(math.log(total)/math.log(10))
            if temp_log > total_log:
                msg_len += temp
                header_size += 2
            total = temp
            A.append(msg_len)

        print (msg_len, header_size)
        return (msg_len/total, total)
