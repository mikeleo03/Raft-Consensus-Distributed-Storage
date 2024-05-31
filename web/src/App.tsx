import { useState } from 'react';
import { Flex, Image, Input, Select, Button, Textarea, Box, useToast } from '@chakra-ui/react';

function App() {

 
  const toast = useToast();


 

  return (
    <Flex direction="column" h="100vh">
      <Flex as="header" backgroundColor="blue.500" backdropFilter="saturate(180%) blur(5px)"
        p={5} alignItems="center" w="100vw">
        
        <Button colorScheme="teal" variant="solid" size="m" ml="auto" minW="10vw" minH="5vh">
          Sistressss
        </Button>
      </Flex>
      <Flex justifyContent="center" alignItems="center" flex="1">
        <Box w="70%">

      <Select placeholder='Select Command'>
        <option value='option1'>ping</option>
        <option value='option2'>append</option>
        <option value='option3'>get</option>
        <option value='option4'>set</option>
        <option value='option5'>request_log</option>
      </Select>
      <Input placeholder='key'/>
      <Input placeholder='value'/>
      <Button colorScheme="teal" variant="solid" size="m" ml="auto" minW="5vw" minH="5vh">
            Submit
      </Button>
        </Box>
      </Flex>
    </Flex>
  )
}

export default App;
