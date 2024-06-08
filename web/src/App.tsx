import { Flex } from '@chakra-ui/react';
import { useForm } from "react-hook-form";
import { z } from "zod";
import { Loader2 } from "lucide-react";
import { toast } from "react-toastify";
import { zodResolver } from "@hookform/resolvers/zod";
import { Form, FormField, FormItem, FormMessage, FormControl } from "@/components/ui/form";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useState } from 'react';

export enum MethodTypeString {
  PING = "Ping",
  APPEND = "Append",
  GET = "Get",
  SET = "Set",
  REQUESTLOG = "Request Log"
}

const FormSchema = z.object({
  key: z.string().min(1, {
    message: "Key is required and cannot be empty.",
  }),

  value: z.string().min(1, {
    message: "Value is required and cannot be empty.",
  }),
});

function App() {
  const [onUpdate, setOnUpdate] = useState<boolean>(false);
  const [commandType, setCommandType] = useState<string>(MethodTypeString.PING);

  const form = useForm<z.infer<typeof FormSchema>>({
      resolver: zodResolver(FormSchema),
      defaultValues: {
        key: "",
        value: ""
      },
  })

  async function onSubmit(data: z.infer<typeof FormSchema>) {
      try {
        // TODO (leon): Handle this
        /* const payload: SubscriptionRequest = {
            name: data.name,
            email: data.email
        }; */
        setOnUpdate(true);

        // Submit the response
        console.log(data);
        toast.success("Thank you for subscribing!");
      } catch (error) {
        toast.error((error as any)?.message || 'Server is unreachable. Please try again later.');
      } finally {
        setOnUpdate(false);
      }
  }

  const handleMethodChange = (type: string) => {
    const value = type as MethodTypeString;
    setCommandType(value);
  };

  return (
    <Flex direction="column" h="100vh">
      <Flex as="header" backgroundColor="blue.500" backdropFilter="saturate(180%) blur(5px)" p={5} alignItems="center" w="100vw">   
        <Button>
          Sistressss
        </Button>
      </Flex>
      <div className='flex flex-col justify-center items-center h-full space-y-6'>
        {/* Title section */}
        <div className='flex flex-col justify-center'>
          <div className='text-3xl font-bold text-center'>Key-value Store</div>
          <div className='text-xl mt-3'>Add new key-value pair to our system!</div>
        </div>

        {/* Input section */}
        <Form {...form}>
            <form onSubmit={form.handleSubmit(onSubmit)} className="w-full space-y-4 justify-center">
                <div className='flex flex-row justify-center items-center px-32 w-full'>
                    <div className='w-1/3'>
                      <Select value={commandType} onValueChange={handleMethodChange}>
                        <SelectTrigger className="w-full h-10 bg-gray-800 border-none text-white">
                            <SelectValue placeholder="Choose Method" />
                        </SelectTrigger>
                        <SelectContent>
                            <SelectItem value={MethodTypeString.PING}>Ping</SelectItem>
                            <SelectItem value={MethodTypeString.APPEND}>Append</SelectItem>
                            <SelectItem value={MethodTypeString.GET}>Get</SelectItem>
                            <SelectItem value={MethodTypeString.SET}>Set</SelectItem>
                            <SelectItem value={MethodTypeString.REQUESTLOG}>Request Log</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <FormField
                        control={form.control}
                        name="key"
                        render={({ field }) => (
                            <FormItem className='w-1/3 rounded-2xl'>
                                <FormControl>
                                    <Input placeholder="Key" {...field} className="md:text-sm text-base border-black" />
                                </FormControl>
                                <FormMessage className="text-left"/>
                            </FormItem>
                        )}
                    />
                    <FormField
                        control={form.control}
                        name="value"
                        render={({ field }) => (
                            <FormItem className='w-1/3 rounded-2xl'>
                                <FormControl>
                                    <Input placeholder="Value" {...field} className="md:text-sm text-base border-black" />
                                </FormControl>
                                <FormMessage className="text-left"/>
                            </FormItem>
                        )}
                    />
                </div>
                <div className="justify-center w-full px-96">
                  <Button type="submit" className="justify-center w-full font-semibold h-fit rounded-3xl text-lg mt-8 mb-3 py-1.5 transition-transform duration-300 transform hover:scale-105" disabled={onUpdate}>
                      {onUpdate ? (
                          <>
                              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                              Submitting
                          </>
                      ) : (
                          'Submit'
                      )}
                  </Button>
                </div>
            </form>
        </Form>
      </div>
    </Flex>
  )
}

export default App;
